#include "request_workers.h"
#include <chrono>
#include <regex>
using namespace ec;
using namespace std::chrono_literals;

int main() {
    try {
        setenv("EC_TRAFFIC", "1", 1);
        Context context;
        std::mutex mutex;
        std::condition_variable ready;
        int bound = 0, arrived = 0;
        constexpr int count = 3, rounds = 12;
        std::vector<std::string> endpoints;
        std::vector<std::future<void>> servers;
        for (int i = 0; i < count; ++i) {
            endpoints.push_back("inproc://requests-" + std::to_string(i));
            servers.push_back(std::async(std::launch::async, [&, endpoint = endpoints.back()] {
                Socket server(context, ZMQ_ROUTER, 5000);
                server.bind(endpoint);
                { std::lock_guard<std::mutex> guard(mutex); ++bound; }
                ready.notify_all();
                std::string identity;
                for (int round = 0; round < rounds; ++round) {
                    auto request = receive(server);
                    require(request.size() == 3 && request[2] == std::to_string(round), "wrong request");
                    if (!round) identity = request[0];
                    require(request[0] == identity, "REQ connection was not reused");
                    {
                        std::unique_lock<std::mutex> guard(mutex);
                        ++arrived;
                        ready.notify_all();
                        // system_clock uses pthread_cond_timedwait, also intercepted
                        // by the host's older TSAN runtime (unlike cond_clockwait).
                        require(ready.wait_until(guard, std::chrono::system_clock::now() + 3s,
                                                [&] { return arrived >= (round + 1) * count; }),
                                "different nodes were serialized");
                    }
                    send(server, {identity, "", round == 4 ? "NOT_FOUND" : "OK", request[2]});
                }
            }));
        }
        {
            std::unique_lock<std::mutex> guard(mutex);
            require(ready.wait_until(guard, std::chrono::system_clock::now() + 3s,
                                    [&] { return bound == count; }), "servers not ready");
        }
        RpcTraffic traffic;
        RequestWorkers workers(context, endpoints);
        for (int round = 0; round < rounds; ++round) {
            std::vector<std::future<Frames>> replies;
            for (int i = 0; i < count; ++i)
                replies.push_back(workers.submit(i, {std::to_string(round)}, 5000, &traffic));
            for (auto& reply : replies)
                require(reply.get() == Frames{round == 4 ? "NOT_FOUND" : "OK", std::to_string(round)}, "wrong reply");
        }
        for (auto& server : servers) server.get();
        require(traffic.requests == count * rounds && traffic.replies == count * rounds, "wrong labelled request counts");
        require(traffic.request_bytes == count * 14 && traffic.reply_bytes == count * 45, "wrong labelled bytes including error replies");
        auto stats = workers.stats();
        for (int i = 0; i < count; ++i)
            require(stats.find("{\"node\":" + std::to_string(i) +
                    ",\"received_bytes\":14,\"sent_bytes\":45,\"total_bytes\":59,\"requests\":12,\"replies\":12}") != std::string::npos,
                    "per-destination link bytes lost or mixed");
        std::regex reused("\"tasks\":12,\"connections\":1,\"resets\":0");
        require(std::distance(std::sregex_iterator(stats.begin(), stats.end(), reused), std::sregex_iterator()) == count,
                "application errors reset the connection");

        // A server applies a request but withholds its reply. Verify no resend,
        // fresh identity after timeout, and no stale reply delivered to the next job.
        std::promise<void> bound_timeout;
        auto bound_future = bound_timeout.get_future();
        auto server = std::async(std::launch::async, [&] {
            Socket router(context, ZMQ_ROUTER, 5000);
            router.bind("inproc://request-timeout");
            bound_timeout.set_value();
            auto old = receive(router);
            require(old.size() == 3 && old[2] == "apply-once", "wrong first request");
            auto next = receive(router);
            require(next.size() == 3 && next[2] == "next" && next[0] != old[0], "request retried or socket not reset");
            send(router, {old[0], "", "OK", "stale"});
            std::this_thread::sleep_for(150ms); // Longer than previous request deadline.
            send(router, {next[0], "", "OK", "next"});
            auto malformed = receive(router);
            require(malformed[0] == next[0] && malformed[2] == "malformed", "connection not reused after recovery");
            send(router, {malformed[0], "", "one-frame"});
            auto drained = receive(router);
            require(drained[0] != next[0] && drained[2] == "drain", "malformed reply did not reset socket");
            send(router, {drained[0], "", "OK", "drained"});
        });
        bound_future.get();
        RpcTraffic failed_traffic;
        RequestWorkers recovering(context, {"inproc://request-timeout"});
        bool failed = false;
        try { recovering.submit(0, {"apply-once"}, 100, &failed_traffic).get(); }
        catch (const Error&) { failed = true; }
        require(failed, "missing reply did not time out");
        require(recovering.submit(0, {"next"}, 3000, &failed_traffic).get() == Frames{"OK", "next"}, "stale reply or incorrect timeout");
        failed = false;
        try { recovering.submit(0, {"malformed"}, 3000, &failed_traffic).get(); }
        catch (const Error&) { failed = true; }
        require(failed, "malformed response accepted");
        auto drained = recovering.submit(0, {"drain"}, 3000, &failed_traffic);
        auto stop1 = std::async(std::launch::async, [&] { recovering.stop(); });
        auto stop2 = std::async(std::launch::async, [&] { recovering.stop(); });
        stop1.get(); stop2.get();
        require(drained.get() == Frames{"OK", "drained"}, "stop lost an accepted job");
        require(recovering.stats().find("\"tasks\":4,\"connections\":3,\"resets\":2") != std::string::npos,
                "wrong reconnection counters");
        failed = false;
        try { recovering.submit(0, {"stopped"}, 3000); }
        catch (const std::runtime_error&) { failed = true; }
        require(failed, "submit after stop accepted");
        server.get();
        require(failed_traffic.requests == 4 && failed_traffic.replies == 3, "timeout or malformed traffic lost");
        require(failed_traffic.request_bytes == 28 && failed_traffic.reply_bytes == 24, "late reply counted or failure bytes lost");
        require(recovering.stats().find("\"received_bytes\":28,\"sent_bytes\":24,\"total_bytes\":52,\"requests\":4,\"replies\":3") != std::string::npos,
                "link counters lost timeout or malformed reply");
        std::promise<void> identity_bound;
        auto identity_ready = identity_bound.get_future();
        auto identity_server = std::async(std::launch::async, [&] {
            Socket router(context, ZMQ_ROUTER, 5000);
            router.bind("inproc://request-identity");
            identity_bound.set_value();
            auto old = receive(router);
            send(router, {old[0], "", "INVALID", "wrong node identity or missing frames"});
            auto next = receive(router);
            require(next[0] != old[0] && next[2] == "next", "identity rejection did not reset or replayed job");
            send(router, {next[0], "", "OK", "correct-node"});
        });
        identity_ready.get();
        RequestWorkers identity_workers(context, {"inproc://request-identity"});
        failed = false;
        try { identity_workers.submit(0, {"first"}, 3000).get(); }
        catch (const Error&) { failed = true; }
        require(failed, "wrong node accepted");
        require(identity_workers.submit(0, {"next"}, 3000).get() == Frames{"OK", "correct-node"}, "identity recovery failed");
        require(identity_workers.stats().find("\"tasks\":2,\"connections\":2,\"resets\":1") != std::string::npos,
                "wrong identity recovery counters");
        identity_server.get();
        std::cout << "PASS: persistent per-node REQ, parallel nodes, application errors, timeout/late reply, no replay, variable timeout, malformed reply, node identity reset and concurrent stop\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
