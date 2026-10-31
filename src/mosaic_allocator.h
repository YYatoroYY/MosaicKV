#pragma once
#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <tuple>
#include <vector>
#include <memory>
#include "metadata_memory.h"
#include <boost/intrusive/set.hpp>

namespace ec::mosaic {
constexpr size_t MAX_ALIGNMENT_CHUNK = 12 * 1024 * 1024;
struct Parameters {
    size_t alignment_chunk = 4096;
    size_t fmax = 4;
    size_t tau = 8;
    void validate() const {
        if (alignment_chunk < 64 || alignment_chunk > MAX_ALIGNMENT_CHUNK || !fmax || fmax > 4096)
            throw std::invalid_argument("invalid Mosaic alignment_chunk/Fmax");
    }
    bool fits(size_t length) const {
        return length / alignment_chunk + (length % alignment_chunk != 0) <= fmax;
    }
};
struct Hole {
    uint64_t chunk = 0;
    uint32_t offset = 0, length = 0;
    Hole() = default;
    Hole(uint64_t id, size_t at, size_t bytes) : chunk(id) {
        if (at > UINT32_MAX || bytes > UINT32_MAX)
            throw std::invalid_argument("fragment exceeds 32-bit chunk range");
        offset = static_cast<uint32_t>(at); length = static_cast<uint32_t>(bytes);
    }
    bool operator==(const Hole& other) const {
        return std::tie(chunk, offset, length) == std::tie(other.chunk, other.offset, other.length);
    }
};
struct ByLength {
    bool operator()(const Hole& a, const Hole& b) const noexcept {
        return std::tie(a.length, a.chunk, a.offset) < std::tie(b.length, b.chunk, b.offset);
    }
};
struct Piece { Hole hole; size_t take; };
struct Plan { uint64_t revision; size_t length; std::vector<Piece> pieces; };

// One owned hole record, indexed by both length and address. Both ordered
// indices are red-black trees; node transfer publishes without allocating.
// Caller serializes access. Plans contain values, never live-index iterators.
class HoleTree {
    struct ByAddress {
        bool operator()(const Hole& a, const Hole& b) const noexcept {
            return std::tie(a.chunk, a.offset) < std::tie(b.chunk, b.offset);
        }
    };
    using Hook = boost::intrusive::set_member_hook<boost::intrusive::optimize_size<true>>;
    struct Entry : Hole {
        Hook by_length, by_address;
        explicit Entry(const Hole& hole) : Hole(hole) {}
    };
    using LengthTree = boost::intrusive::set<Entry,
        boost::intrusive::member_hook<Entry, Hook, &Entry::by_length>,
        boost::intrusive::compare<ByLength>>;
    using AddressTree = boost::intrusive::set<Entry,
        boost::intrusive::member_hook<Entry, Hook, &Entry::by_address>,
        boost::intrusive::compare<ByAddress>>;
    LengthTree holes_;
    AddressTree addresses_;
    uint64_t hole_heap_bytes_ = 0;
    void erase_hole(const Hole& hole) noexcept {
        auto it = holes_.find(hole, ByLength{});
        if (it == holes_.end()) std::terminate();
        auto* entry = &*it;
        hole_heap_bytes_ -= metadata_memory::allocation(entry);
        holes_.erase(it);
        addresses_.erase(addresses_.iterator_to(*entry));
        delete entry;
    }
    // Production trees use one fixed chunk size and only keep chunk IDs.
    // Nonuniform sizes are supported for allocator tests through sparse overrides.
    std::vector<uint64_t> chunks_;
    std::map<uint64_t, uint32_t> size_overrides_;
    uint32_t chunk_bytes_ = 0;
    uint64_t revision_ = 0;
    size_t free_bytes_ = 0;
    size_t bytes(uint64_t id) const {
        auto it = size_overrides_.find(id);
        return it == size_overrides_.end() ? chunk_bytes_ : it->second;
    }
    void replace(const std::vector<Hole>& old, const std::vector<Hole>& fresh) {
        std::vector<std::unique_ptr<Entry>> staged;
        size_t removed = 0, added = 0;
        // Allocate every new record before changing either live index.
        for (const auto& h : fresh) {
            staged.push_back(std::make_unique<Entry>(h));
            added += h.length;
        }
        for (const auto& h : old) { erase_hole(h); removed += h.length; }
        for (auto& entry : staged) {
            if (!holes_.insert(*entry).second || !addresses_.insert(*entry).second)
                std::terminate(); // Internal overlap/index bug.
            hole_heap_bytes_ += metadata_memory::allocation(entry.get());
            entry.release(); // Both indices share this one owned record.
        }
        free_bytes_ = free_bytes_ - removed + added;
        ++revision_;
    }
public:
    HoleTree() = default;
    HoleTree(const HoleTree&) = delete;
    HoleTree& operator=(const HoleTree&) = delete;
    ~HoleTree() {
        holes_.clear_and_dispose([&](Entry* entry) {
            addresses_.erase(addresses_.iterator_to(*entry));
            delete entry;
        });
    }
    void swap(HoleTree& other) noexcept {
        holes_.swap(other.holes_); addresses_.swap(other.addresses_);
        chunks_.swap(other.chunks_); size_overrides_.swap(other.size_overrides_);
        std::swap(chunk_bytes_, other.chunk_bytes_);
        std::swap(revision_, other.revision_); std::swap(free_bytes_, other.free_bytes_);
        std::swap(hole_heap_bytes_, other.hole_heap_bytes_);
    }
    HoleTree(HoleTree&& other) noexcept { swap(other); }
    HoleTree& operator=(HoleTree&& other) noexcept {
        if (this != &other) { HoleTree moved(std::move(other)); swap(moved); }
        return *this;
    }
    uint64_t metadata_bytes() const {
        uint64_t bytes = metadata_memory::vector(chunks_) + metadata_memory::nodes(size_overrides_);
        return bytes + hole_heap_bytes_;
    }
    uint64_t rb_tree_metadata_bytes() const {
        // Two intrusive hooks per hole: three pointers each, color packed in
        // the parent pointer. Shared Hole payload is excluded from this census.
        return holes_.size() * 2 * 3 * sizeof(void*) + metadata_memory::rb_tree_fields(size_overrides_);
    }
    uint64_t useful_metadata_bytes() const {
        // One range record shared by both indices; structural hooks excluded.
        // Include state of occupied coefficient groups, not unused group slots.
        return holes_.size() * (8 + 4 + 4) + chunks_.size() * 8 + size_overrides_.size() * (8 + 4)
            + (chunks_.empty() ? 0 : 4 + 8 + 8); // chunk_bytes, revision, free_bytes
    }
    uint64_t revision() const { return revision_; }
    size_t free_bytes() const { return free_bytes_; }
    size_t hole_count() const { return holes_.size(); }
    size_t chunk_count() const { return chunks_.size(); }
    size_t capacity(size_t fmax) const {
        size_t result = 0;
        for (auto it = holes_.rbegin(); it != holes_.rend() && fmax; ++it, --fmax) result += it->length;
        return result;
    }
    std::vector<Hole> holes() const { return {holes_.begin(), holes_.end()}; }
    void add_chunk(uint64_t id, size_t count) {
        if (!count || count > UINT32_MAX || std::binary_search(chunks_.begin(), chunks_.end(), id))
            throw std::invalid_argument("invalid/duplicate chunk");
        auto index = std::lower_bound(chunks_.begin(), chunks_.end(), id) - chunks_.begin();
        chunks_.insert(chunks_.begin() + index, id);
        try {
            if (!chunk_bytes_) chunk_bytes_ = static_cast<uint32_t>(count);
            if (count != chunk_bytes_) size_overrides_.emplace(id, static_cast<uint32_t>(count));
            replace({}, {{id, 0, count}});
        } catch (...) { size_overrides_.erase(id); chunks_.erase(chunks_.begin() + index); throw; }
    }
    bool remove_empty_chunk(uint64_t id) noexcept {
        auto chunk = std::lower_bound(chunks_.begin(), chunks_.end(), id);
        if (chunk == chunks_.end() || *chunk != id) return false;
        auto count = bytes(id);
        auto hole = holes_.find(Hole{id, 0, count}, ByLength{});
        if (hole == holes_.end()) return false;
        erase_hole(*hole); chunks_.erase(chunk); size_overrides_.erase(id);
        free_bytes_ -= count;
        ++revision_;
        return true;
    }
    std::optional<Plan> allocate(size_t length, size_t fmax = 4, size_t tau = 8) const {
        if (!fmax) throw std::invalid_argument("Fmax must be positive");
        Plan result{revision_, length, {}};
        size_t remaining = length, budget = fmax;
        std::set<Hole, ByLength> removed; // Only this request's choices, O(Fmax).
        auto lower_bound = [&](size_t minimum) {
            auto it = minimum > UINT32_MAX ? holes_.end() : holes_.lower_bound(Hole{0, 0, minimum}, ByLength{});
            while (it != holes_.end() && removed.count(*it)) ++it;
            return it;
        };
        while (remaining) {
            if (!budget) return std::nullopt;
            auto h = lower_bound(remaining);
            if (h != holes_.end() && h->length == remaining) {
                result.pieces.push_back({*h, remaining});
                return result;
            }
            h = lower_bound(remaining / budget + (remaining % budget != 0));
            if (h == holes_.end()) return std::nullopt;
            auto difference = h->length > remaining ? h->length - remaining : remaining - h->length;
            if (difference && difference < tau && remaining <= std::numeric_limits<size_t>::max() - tau) {
                auto alternative = lower_bound(remaining + tau);
                if (alternative != holes_.end()) h = alternative;
            }
            size_t take = std::min<size_t>(h->length, remaining);
            result.pieces.push_back({*h, take});
            removed.insert(*h);
            remaining -= take;
            --budget;
        }
        return result;
    }
    bool commit(const Plan& plan) {
        if (plan.revision != revision_) return false;
        std::vector<Hole> old, tails;
        std::set<std::pair<uint64_t, uint32_t>> selected;
        size_t remaining = plan.length;
        for (const auto& p : plan.pieces) {
            if (!p.take || p.take > p.hole.length || p.take > remaining || !holes_.count(p.hole, ByLength{}) ||
                !selected.emplace(p.hole.chunk, p.hole.offset).second) return false;
            remaining -= p.take;
            old.push_back(p.hole);
            if (p.take < p.hole.length) tails.push_back({p.hole.chunk, p.hole.offset + p.take, p.hole.length - p.take});
        }
        if (remaining) return false;
        replace(old, tails);
        return true;
    }
    // Ownership lives in Object::fragments (or the current reserved Plan).
    // Reject out-of-range/overlapping/double frees without a second allocation map.
    // The caller must release only owned ranges, not arbitrary occupied subranges.
    void release(const Hole& used) {
        if (!used.length || !std::binary_search(chunks_.begin(), chunks_.end(), used.chunk) || used.offset > bytes(used.chunk) ||
            used.length > bytes(used.chunk) - used.offset)
            throw std::invalid_argument("invalid release range");
        Hole merged = used;
        std::vector<Hole> old;
        const auto& addresses = addresses_;
        auto next = addresses.lower_bound(used, ByAddress{});
        const uint64_t end = uint64_t(used.offset) + used.length;
        if (next != addresses.end() && next->chunk == used.chunk) {
            if (end > next->offset) throw std::invalid_argument("release overlaps free space");
            if (end == next->offset) { old.push_back(*next); merged.length += next->length; }
        }
        if (next != addresses.begin()) {
            auto prev = std::prev(next);
            if (prev->chunk == used.chunk) {
                const uint64_t prev_end = uint64_t(prev->offset) + prev->length;
                if (prev_end > used.offset) throw std::invalid_argument("release overlaps free space");
                if (prev_end == used.offset) {
                    old.push_back(*prev); merged.offset = prev->offset; merged.length += prev->length;
                }
            }
        }
        replace(old, {merged}); // Never merge across chunk boundaries.
    }
};

// Existing cyclic placement, with a stripe's start rotating across nodes. A
// coefficient class is its Cauchy matrix data column, NOT a physical node ID.
class Layout {
    int k_, m_, nodes_;
public:
    Layout(int k, int m, int nodes) : k_(k), m_(m), nodes_(nodes) {
        if (k < 1 || m < 1 || k + m > 32 || nodes < k + m || nodes > 256)
            throw std::invalid_argument("invalid Mosaic layout");
    }
    int node(int start, int index) const {
        if (start < 0 || start >= nodes_ || index < 0 || index >= k_ + m_)
            throw std::invalid_argument("invalid stripe position");
        return (start + index) % nodes_;
    }
    int start_for(int node, int coefficient) const {
        if (node < 0 || node >= nodes_ || coefficient < 0 || coefficient >= k_)
            throw std::invalid_argument("invalid coefficient class");
        return (node - coefficient + nodes_) % nodes_;
    }
};
} // namespace ec::mosaic
