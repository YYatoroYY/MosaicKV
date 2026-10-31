#pragma once
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include "metadata_memory.h"

namespace ec::mosaic {
// Cumulative acknowledged payload since the common test/Proxy start epoch.
// Ranking bytes is equivalent to ranking bandwidth with the same denominator.
// Recording writes never allocates, and idle time never expires the counters.
class CumulativeBandwidth {
    std::vector<uint64_t> nodes_;
    uint64_t started_ms_;
public:
    CumulativeBandwidth(size_t nodes, uint64_t started_ms) : nodes_(nodes, 0), started_ms_(started_ms) {
        if (!nodes) throw std::invalid_argument("invalid bandwidth node count");
    }
    void record(size_t node, uint64_t bytes) { nodes_.at(node) += bytes; }
    void reset(uint64_t started_ms) { std::fill(nodes_.begin(), nodes_.end(), 0); started_ms_ = started_ms; }
    uint64_t useful_metadata_bytes() const { return nodes_.size() * sizeof(uint64_t); }
    uint64_t metadata_bytes() const { return metadata_memory::vector(nodes_); }
    uint64_t bytes(size_t node) const { return nodes_.at(node); }
    double bytes_per_second(size_t node, uint64_t now_ms) const {
        return static_cast<double>(bytes(node)) * 1000.0 / (now_ms > started_ms_ ? now_ms - started_ms_ : 1);
    }
};
} // namespace ec::mosaic
