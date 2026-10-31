#pragma once
#include "common.h"
#include <map>

namespace ec {
// Monotonic IDs: <random proxy session>_<hex sequence>[_parity lane].
// Retain one high-water mark per stream, not a tombstone per user update.
// A process restart loses this volatile journal and requires fencing/recovery;
// it must never be treated as a successful transaction retry.
template<class T> class TransactionJournal {
    struct Slot { uint64_t sequence = 0; T value; };
    std::map<std::string, Slot> streams_;
public:
    T& get(const std::string& id) {
        const auto split = id.find('_'), end = id.find('_', split + 1);
        require(split == 32 && id.size() > 33 && id.size() <= 80, "invalid monotonic transaction ID");
        const auto number = id.substr(split + 1, end == std::string::npos ? end : end-split-1);
        size_t used = 0; const auto seq = std::stoull(number, &used, 16);
        require(seq && used == number.size(), "invalid transaction sequence");
        auto stream = id.substr(0, split) + (end == std::string::npos ? "" : id.substr(end));
        auto& slot = streams_[stream];
        require(seq >= slot.sequence, "stale transaction sequence");
        if (seq > slot.sequence) {
            require(!slot.sequence || (slot.value.terminal && !slot.value.busy), "previous transaction is not settled");
            slot.value = T{}; slot.sequence = seq;
        }
        return slot.value;
    }
};
} // namespace ec
