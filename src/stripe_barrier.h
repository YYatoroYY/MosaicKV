#pragma once
#include "common.h"
#include <condition_variable>
#include <mutex>

namespace ec {
// Counter tokens can survive a failed request and be settled by another
// thread. Unlike a thread-owned shared_mutex, this also fences unknown writes.
class StripeBarrier {
    struct State { size_t updates = 0, readers = 0; };
    std::mutex mutex_;
    std::condition_variable changed_;
    std::map<uint64_t, State> states_;
public:
    void enter(const std::vector<uint64_t>& ids, bool update, int timeout) {
        std::unique_lock<std::mutex> l(mutex_);
        // Reserve every record before modifying counters (strong guarantee).
        for (auto id : ids) states_.try_emplace(id);
        const auto ready = [&] { for (auto id : ids) {
            const auto& s = states_.at(id);
            if (update ? s.readers != 0 : s.updates != 0) return false;
        } return true; };
        if (!changed_.wait_for(l, std::chrono::milliseconds(timeout), ready))
            throw Error("UNAVAILABLE", "stripe has unsettled parity updates or recovery readers");
        for (auto id : ids) { auto& s = states_.at(id); if (update) ++s.updates; else ++s.readers; }
    }
    void leave(const std::vector<uint64_t>& ids, bool update) noexcept {
        { std::lock_guard<std::mutex> l(mutex_);
          for (auto id : ids) { auto& s = states_.at(id); if (update) --s.updates; else --s.readers; } }
        changed_.notify_all();
    }
};
} // namespace ec
