#pragma once
#include "common.h"
#include <unordered_map>

namespace ec {
// Only shards partially restored after loss have entries. Unknown bytes are
// erasures, never implicit zeros. Node REP serializes publication and reads.
class PartialRanges {
    using Range = std::pair<size_t, size_t>;
    std::unordered_map<std::string, std::vector<Range>> valid_;
public:
    void begin(const std::string& key) { valid_.try_emplace(key); }
    void erase(const std::string& key) { valid_.erase(key); }
    size_t size() const { return valid_.size(); }
    bool has(const std::string& key, size_t offset, size_t length) const {
        if (valid_.empty() || !length) return true;
        auto it = valid_.find(key);
        if (it == valid_.end()) return true;
        for (auto [begin, end] : it->second)
            if (begin <= offset && offset <= end && length <= end - offset) return true;
        return false;
    }
    void check(const std::string& key, size_t offset, size_t length) const {
        if (!has(key, offset, length)) throw Error("NOT_FOUND", "shard range not yet repaired");
    }
    void restored(const std::string& key, size_t offset, size_t length, size_t total) {
        auto it = valid_.find(key);
        if (it == valid_.end()) return;
        auto ranges = it->second;
        ranges.emplace_back(offset, offset + length);
        std::sort(ranges.begin(), ranges.end());
        std::vector<Range> merged;
        for (auto r : ranges) {
            if (!merged.empty() && r.first <= merged.back().second)
                merged.back().second = std::max(merged.back().second, r.second);
            else merged.push_back(r);
        }
        if (merged.size() == 1 && merged[0] == Range{0, total}) valid_.erase(it);
        else it->second.swap(merged);
    }
};
} // namespace ec
