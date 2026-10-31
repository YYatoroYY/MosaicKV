#include "parity_workers.h"
#include <chrono>
using namespace ec;
using namespace std::chrono_literals;

// An alarm bounds broken joins as well as network waits, so regressions fail CI.
int main() {
    alarm(45);
    try {
        Context context;
        constexpr int count = 3, producers = 8, rounds = 40;
        std::atomic<bool> finish{false};
        std::vector<std::future<void>> servers;
        for (int i = 0; i < count; ++i) {
            std::promise<void> bound;
            auto ready = bound.get_future();
            servers.push_back(std::async(std::launch::async, [&, i, bound = std::move(bound)]() mutable {
                Socket socket(context, ZMQ_REP, 100);
                socket.bind("inproc://concurrent-" + std::to_string(i));
                bound.set_value();
                while (!finish) {
                    Frames request;
                    try { request = receive(socket); }
                    catch (const Error&) { if (finish) break; continue; }
                    send(socket, {"OK", request.at(0)});
                }
            }));
            ready.get();
        }
        ParityWorkers workers(context, count, 2000);
        std::vector<std::future<void>> clients;
        for (int p = 0; p < producers; ++p) clients.push_back(std::async(std::launch::async, [&, p] {
            for (int r = 0; r < rounds; ++r) {
                PendingReplies pending(count);
                auto value = std::to_string(p) + ":" + std::to_string(r);
                for (int i = 0; i < count; ++i)
                    pending.add(workers.submit(i, "inproc://concurrent-" + std::to_string(i), {value}));
                for (auto& reply : pending) require(reply.get().at(1) == value, "crossed producer replies");
                require(!workers.stats().empty(), "empty concurrent stats");
            }
        }));
        for (auto& client : clients) client.get();

        // Keep one accepted request in flight while multiple callers race stop()
        // against submissions. The receiver socket is confined to its own thread.
        std::promise<void> bound, arrived, release;
        auto released = release.get_future().share();
        auto blocked = std::async(std::launch::async, [&] {
            Socket socket(context, ZMQ_REP, 5000);
            socket.bind("inproc://held");
            bound.set_value();
            auto request = receive(socket);
            arrived.set_value();
            released.wait();
            send(socket, {"OK", request.at(0)});
        });
        bound.get_future().get();
        auto held = workers.submit(0, "inproc://held", {"held"});
        arrived.get_future().get();
        std::promise<void> start;
        auto started = start.get_future().share();
        std::vector<std::future<void>> stoppers;
        for (int i = 0; i < 4; ++i) stoppers.push_back(std::async(std::launch::async, [&] {
            started.wait();
            workers.stop();
        }));
        auto submitter = std::async(std::launch::async, [&] {
            started.wait();
            PendingReplies pending(100);
            for (int i = 0; i < 100; ++i) {
                try { pending.add(workers.submit(0, "inproc://concurrent-0", {"race"})); }
                catch (const std::runtime_error&) { break; }
            }
            for (auto& reply : pending) require(reply.get().at(1) == "race", "lost accepted job");
        });
        start.set_value();
        bool premature = stoppers[0].wait_for(100ms) == std::future_status::ready;
        release.set_value();
        require(!premature, "stop returned with an active job");
        require(held.get().at(1) == "held", "shutdown lost active job");
        for (auto& stopper : stoppers) stopper.get();
        submitter.get();
        blocked.get();
        bool rejected = false;
        try { workers.submit(0, "inproc://concurrent-0", {"late"}); }
        catch (const std::runtime_error&) { rejected = true; }
        require(rejected, "submission after stop accepted");
        workers.stop();
        finish = true;
        for (auto& server : servers) server.get();

        // A later submission failure must drain earlier promise-backed jobs before
        // exception handlers can retire or free batch-owned state.
        ParityWorkers batch_workers(context, 1, 5000);
        std::promise<void> batch_bound, batch_arrived, throwing, batch_release;
        auto batch_released = batch_release.get_future().share();
        auto batch_server = std::async(std::launch::async, [&] {
            Socket socket(context, ZMQ_REP, 5000);
            socket.bind("inproc://batch");
            batch_bound.set_value();
            receive(socket);
            batch_arrived.set_value();
            batch_released.wait();
            send(socket, {"OK", "done"});
        });
        batch_bound.get_future().get();
        auto batch = std::async(std::launch::async, [&] {
            try {
                PendingReplies pending(1);
                pending.add(batch_workers.submit(0, "inproc://batch", {"write"}));
                batch_arrived.get_future().get();
                throwing.set_value();
                throw std::runtime_error("injected failure before next submit");
            } catch (const std::runtime_error&) {}
        });
        throwing.get_future().get();
        bool escaped = batch.wait_for(100ms) == std::future_status::ready;
        batch_release.set_value();
        batch.get();
        batch_server.get();
        require(!escaped, "exception escaped before accepted batch drained");
        std::cout << "PASS: 960 concurrent jobs, stats, four concurrent stops, submit/stop race, exception drain\n";
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
