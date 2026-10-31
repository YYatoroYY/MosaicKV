#include "native_io.h"
#include <cassert>
#include <chrono>
using namespace ec;
int main() {
    NativeIo io(4);
    std::mutex mutex;
    std::condition_variable entered;
    size_t arrived = 0;
    std::set<std::thread::id> tids;
    io.run(4, NativeIo::Op::Read, [&](size_t) {
        std::unique_lock<std::mutex> lock(mutex);
        ++arrived; tids.insert(std::this_thread::get_id()); entered.notify_all();
        if (!entered.wait_for(lock, std::chrono::seconds(5), [&] { return arrived == 4; }))
            throw std::runtime_error("native jobs failed to overlap");
    });
    assert(tids.size() == 4);
    assert(io.stats().find("\"max_active\":4") != std::string::npos);
    std::atomic<int> completed{0};
    bool failed = false;
    try {
        io.run(32, NativeIo::Op::Write, [&](size_t i) {
            if (!i) throw std::runtime_error("injected job failure");
            ++completed;
        });
    } catch (const std::runtime_error&) { failed = true; }
    assert(failed && completed == 31); // Includes all jobs after the failed lane.
    io.run(4, NativeIo::Op::Xor, [&](size_t) { ++completed; });
    assert(completed == 35);
    for (int i = 0; i < 100; ++i) io.run(8, NativeIo::Op::Delta, [&](size_t) { ++completed; });
    assert(completed == 835);
    std::cout << "PASS native pool: four concurrent persistent workers, failure drain, continued batches\n";
}
