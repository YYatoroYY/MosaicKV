#pragma once
#include <cstdint>
#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace ec {
// Maximum of simultaneous active-node-payload/live ratios, not a ratio of
// independently maximized byte counts. Transient and retired copies are outside
// this metric, matching STATS active_node_payload_bytes.
struct GcAmplificationPeak {
    uint64_t payload = 0, live = 0, data = 0, parity = 0, replicas = 0;
    uint64_t observations = 0, observation_at_peak = 0;
    void observe(uint64_t data_bytes, uint64_t parity_bytes, uint64_t replica_bytes,
                 uint64_t live_bytes) noexcept {
        ++observations;
        if (!live_bytes) return; // The reference is undefined for an empty workload.
        const auto bytes = data_bytes + parity_bytes + replica_bytes;
        // GCC's 128-bit integer avoids overflow and floating-point comparisons.
        if (!live || __uint128_t(bytes) * live > __uint128_t(payload) * live_bytes) {
            payload = bytes; live = live_bytes; data = data_bytes;
            parity = parity_bytes; replicas = replica_bytes;
            observation_at_peak = observations;
        }
    }
    double normalized(int k, int m) const noexcept {
        return live ? static_cast<double>(payload) / live * k / (k + m) : 0;
    }
};
// Peaks of committed active data capacity, never parity or transient GC copies.
// Observe once after an atomic publication, not between individual index edits.
struct GcSpacePeaks {
    uint64_t padding = 0, invalid = 0, waste = 0, live_at_padding = 0, live_at_waste = 0;
    uint64_t observations = 0;
    void observe(uint64_t total, uint64_t dead, uint64_t ec_live, uint64_t live) noexcept {
        const auto pad = total - dead - ec_live;
        if (!observations || pad > padding) { padding = pad; live_at_padding = live; }
        if (dead > invalid) invalid = dead;
        // Max of simultaneous sums, not the sum of independently measured maxima.
        if (!observations || pad + dead > waste) { waste = pad + dead; live_at_waste = live; }
        ++observations;
    }
};
// Protected by Store::mutex_. Prepare allocates only affected records; apply
// transfers those nodes after catalog publication without allocating memory.
class GcIndex {
public:
    struct Entry {
        int start = 0, data = 0, bytes = 0;
        uint32_t mask = 0;
        uint64_t logical = 0;
        std::vector<size_t> lengths = {}; // Empty only for legacy fixed-width accounting.
    };
    struct Edit {
        std::set<std::string> affected, dirty, empty;
        std::map<std::string, Entry> entries;
        void prepare() {
            for (const auto& [id, e] : entries) {
                if (!e.mask) empty.insert(id);
                else if (__builtin_popcount(e.mask) < e.data) dirty.insert(id);
            }
        }
    };
    std::map<std::string, Entry> entries;
    std::set<std::string> dirty, empty;
    uint64_t total = 0, invalid = 0, parity = 0, logical = 0;
    std::vector<uint64_t> node_bytes;
    int m = 0;
    GcIndex(size_t nodes = 0, int parity_count = 0) : node_bytes(nodes), m(parity_count) {}
    void account(const Entry& e, bool add) noexcept {
        auto change = [add](uint64_t& value, uint64_t amount) {
            if (add) value += amount; else value -= amount;
        };
        size_t parity_bytes = e.lengths.empty() ? e.bytes : *std::max_element(e.lengths.begin(), e.lengths.end());
        for (int i = 0; i < e.data; ++i) {
            size_t length = e.lengths.empty() ? e.bytes : e.lengths[i];
            change(total, length);
            if (!(e.mask & (uint32_t(1) << i))) change(invalid, length);
        }
        change(parity, uint64_t(m) * parity_bytes);
        change(logical, e.logical);
        for (int i = 0; i < e.data + m; ++i) change(node_bytes[(e.start + i) % node_bytes.size()],
                i < e.data ? (e.lengths.empty() ? e.bytes : e.lengths[i]) : parity_bytes);
    }
    void apply(Edit edit) {
        for (const auto& id : edit.affected) {
            auto old = entries.find(id);
            if (old != entries.end()) { account(old->second, false); entries.erase(old); }
            dirty.erase(id); empty.erase(id);
        }
        while (!edit.entries.empty()) {
            auto node = edit.entries.extract(edit.entries.begin());
            account(node.mapped(), true);
            entries.insert(std::move(node));
        }
        while (!edit.dirty.empty()) dirty.insert(edit.dirty.extract(edit.dirty.begin()));
        while (!edit.empty.empty()) empty.insert(edit.empty.extract(edit.empty.begin()));
    }
};
} // namespace ec
