#include "concurrent_index.h"
#include "stripe_barrier.h"
#include <future>
#include <thread>
#include <iostream>

int main() {
    try {
        ec::ConcurrentIndex<int, int> index;
        std::vector<std::thread> threads;
        for (int worker = 0; worker < 8; ++worker) threads.emplace_back([&, worker] {
            for (int i = 0; i < 500; ++i) {
                auto key = std::to_string(worker*500+i);
                auto e = index.find(key, true);
                std::lock_guard<std::mutex> write(e->writer);
                e->publish(index.version(i));
                if (*index.find(key)->load() != i) std::terminate();
            }
        });
        for (auto& t : threads) t.join();
        ec::require(index.snapshot().size() == 4000, "concurrent hash insertion lost keys");
        auto e = index.find("0");
        auto pin = e->load();
        e->publish(index.version(900));
        ec::require(*pin == 0 && *e->load() == 900, "published map changed a reader's version");
        e->publish({});
        auto other = index.find("0");
        index.prune("0", e);
        ec::require(bool(index.find("0")), "pruned an entry held by another request");
        other.reset(); index.prune("0", e);
        ec::require(!index.find("0") && *pin == 0, "retired version was not independently pinned");

        ec::StripeBarrier barrier;
        barrier.enter({1, 2}, true, 100);
        barrier.enter({2}, true, 100);
        bool rejected = false;
        try { barrier.enter({2}, false, 10); } catch (const ec::Error&) { rejected = true; }
        ec::require(rejected, "decoded during in-flight parity updates");
        barrier.leave({1, 2}, true);
        rejected = false;
        try { barrier.enter({2}, false, 10); } catch (const ec::Error&) { rejected = true; }
        ec::require(rejected, "one completed writer released another writer's fence");
        barrier.leave({2}, true);
        barrier.enter({1, 2}, false, 100);
        rejected = false;
        try { barrier.enter({1}, true, 10); } catch (const ec::Error&) { rejected = true; }
        ec::require(rejected, "writer entered during decoding");
        barrier.leave({1, 2}, false);
        barrier.enter({1}, true, 100); barrier.leave({1}, true);
        std::cout << "PASS: concurrent hash publication, reader pins, tombstone lifecycle and stripe fences\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
