#pragma once
#include <map>
#include <optional>
#include <string>

namespace ec {
// Owned by the Agent REP thread. Group totals describe the same observed
// entries as the length index, including zero-length retirement markers.
class LengthIndex {
public:
    struct Usage { size_t entries = 0, key_bytes = 0, value_bytes = 0; };
private:
    std::map<std::string, size_t> entries_;
    std::map<std::string, Usage> groups_;
    static std::string group(const std::string& key) { return key.substr(0, key.find('/')); }
public:
    void record(const std::string& key, size_t length) {
        auto old = entries_.find(key);
        if (old != entries_.end()) {
            if (old->second == length) return;
            auto& usage = groups_.at(group(key));
            usage.value_bytes = usage.value_bytes - old->second + length;
            old->second = length;
            return;
        }
        auto [g, inserted] = groups_.try_emplace(group(key));
        try { entries_.emplace(key, length); }
        catch (...) { if (inserted) groups_.erase(g); throw; }
        ++g->second.entries;
        g->second.key_bytes += key.size();
        g->second.value_bytes += length;
    }
    void erase(const std::string& key) {
        auto old = entries_.find(key);
        if (old == entries_.end()) return;
        auto g = groups_.find(group(key));
        --g->second.entries;
        g->second.key_bytes -= old->first.size();
        g->second.value_bytes -= old->second;
        if (!g->second.entries) groups_.erase(g);
        entries_.erase(old);
    }
    std::optional<size_t> size(const std::string& key) const {
        auto it = entries_.find(key);
        if (it == entries_.end()) return std::nullopt;
        return it->second;
    }
    const auto& entries() const { return entries_; }
    const auto& groups() const { return groups_; }
};
} // namespace ec
