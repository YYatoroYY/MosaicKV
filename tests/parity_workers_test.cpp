#include "parity_workers.h"
#include <chrono>
#include <regex>
using namespace ec;

int main() {
    try {
        setenv("EC_TRAFFIC", "1", 1);
        Context context;
        std::mutex mutex;
        std::condition_variable condition;
        int arrived = 0;
        constexpr int count = 3, rounds = 12;
        std::vector<std::unique_ptr<Socket>> servers;
        for (int i = 0; i < count; ++i) {
            servers.push_back(std::make_unique<Socket>(context, ZMQ_REP, 5000));
            servers.back()->bind("inproc://parity-test-" + std::to_string(i));
        }
        std::vector<std::future<void>> receivers;
        for (int i = 0; i < count; ++i) receivers.push_back(std::async(std::launch::async, [&, i] {
            for (int round = 0; round < rounds; ++round) {
                receive(*servers[i]);
                {
                    std::unique_lock<std::mutex> guard(mutex);
                    ++arrived;
                    condition.notify_all();
                    // No response until all M writes have arrived: serialization would time out.
                    if (!condition.wait_for(guard, std::chrono::seconds(3), [&] { return arrived >= (round + 1) * count; }))
                        throw std::runtime_error("parity requests did not overlap");
                }
                send(*servers[i], {"OK", std::to_string(round)});
            }
        }));
        ParityWorkers workers(context, count, 5000);
        std::regex tids("\"tid\":[0-9]+");
        auto before = workers.stats();
        std::vector<std::string> expected;
        for (std::sregex_iterator it(before.begin(), before.end(), tids), end; it != end; ++it) expected.push_back(it->str());
        for (int round = 0; round < rounds; ++round) {
            std::vector<std::future<Frames>> pending;
            for (int i = 0; i < count; ++i)
                pending.push_back(workers.submit(i, "inproc://parity-test-" + std::to_string(i), {"PATCH"}));
            for (auto& task : pending) if (task.get()[1] != std::to_string(round)) throw std::runtime_error("wrong reply");
        }
        for (auto& receiver : receivers) receiver.get();
        require(workers.sent_bytes() == count * rounds * 5, "wrong parity request byte count");
        uint64_t reply_bytes = 0;
        for (int round = 0; round < rounds; ++round) reply_bytes += count * (2 + std::to_string(round).size());
        require(workers.received_bytes() == reply_bytes, "wrong parity reply byte count");
        auto after = workers.stats();
        std::vector<std::string> actual;
        for (std::sregex_iterator it(after.begin(), after.end(), tids), end; it != end; ++it) actual.push_back(it->str());
        if (actual != expected || after.find("\"max_parallel_writes\":3") == std::string::npos)
            throw std::runtime_error("workers not reused or no parallel writes");
        std::regex reused("\"tasks\":12,\"connections\":1");
        if (std::distance(std::sregex_iterator(after.begin(), after.end(), reused), std::sregex_iterator()) != count)
            throw std::runtime_error("connections were not reused");
        // Retries and error replies belong to their original maintenance category.
        RpcTraffic traffic;
        Socket retry_server(context, ZMQ_REP, 5000);
        retry_server.bind("inproc://parity-category");
        auto retry_receiver = std::async(std::launch::async, [&] {
            require(receive(retry_server) == Frames{"WRITE", "0", "block", "data"}, "wrong first write");
            send(retry_server, {"UNAVAILABLE", "retry"});
            require(receive(retry_server) == Frames{"WRITE", "0", "block", "data"}, "retry changed request");
            send(retry_server, {"OK", ""});
            require(receive(retry_server) == Frames{"PING"}, "missing diagnostic");
            send(retry_server, {"OK", "pong"});
        });
        workers.submit(0, "inproc://parity-category", {"WRITE", "0", "block", "data"}, &traffic, 2).get();
        workers.submit(0, "inproc://parity-category", {"PING"}, &traffic, 2).get();
        retry_receiver.get();
        require(traffic.requests == 2 && traffic.replies == 2, "retry counts or diagnostic exclusion wrong");
        require(traffic.request_bytes == 30 && traffic.reply_bytes == 18, "retry/error frame bytes wrong");
        require(workers.stats().find("{\"node\":2,\"received_bytes\":30,\"sent_bytes\":18,\"total_bytes\":48,\"requests\":2,\"replies\":2}") != std::string::npos,
                "retry link attribution or diagnostic exclusion wrong");
        // Neither XOR protocol has replay records. Only absolute/idempotent
        // operations such as WRITE retain their transport retry behavior.
        for (const auto& op : Frames{"PATCH", "PATCH_BATCH", "PATCH_RESIZE"}) {
            auto receiver = std::async(std::launch::async, [&] {
                require(receive(retry_server) == Frames{op}, "missing patch");
                send(retry_server, {"UNAVAILABLE", "partial write"});
                require(receive(retry_server) == Frames{"PING"}, "unsafe XOR retry");
                send(retry_server, {"OK", "pong"});
            });
            bool failed = false;
            try { workers.submit(0, "inproc://parity-category", {op}).get(); }
            catch (const Error&) { failed = true; }
            require(failed, "patch error was suppressed");
            workers.submit(0, "inproc://parity-category", {"PING"}).get();
            receiver.get();
        }
        std::cout << "PASS: persistent parallel workers; absolute writes retry; single/batch XOR never retried\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
