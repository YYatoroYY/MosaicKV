#pragma once
#include "mosaic_allocator.h"
#include <iterator>
#include <new>

namespace ec::mosaic {
// Immutable published fragments: four compact descriptors (or two wide ones)
// inline in a fixed 40-byte object. Larger arrays allocate exactly their count.
// Compact ranges cover <=32-bit IDs and <=64KiB; other ranges use 16 bytes.
class CompactObject {
    struct SmallFragment { uint32_t chunk; uint16_t offset, length_minus_one; };
    static_assert(sizeof(SmallFragment) == 8);
    static constexpr uint16_t compact_bit = 0x8000, count_mask = 0x7fff;
    struct SmallArray { SmallFragment data[4]; };
    struct WideArray { Hole data[2]; };
    union Storage {
        SmallArray small;
        WideArray wide;
        void* many;
        Storage() : wide{} {}
    } storage_;
    uint16_t flags_ = 0;
    size_t count() const { return flags_ & count_mask; }
    bool compact() const { return flags_ & compact_bit; }
    bool heap() const { return count() > (compact() ? 4 : 2); }
    void release() noexcept {
        if (heap()) {
            if (compact()) delete[] static_cast<SmallFragment*>(storage_.many);
            else delete[] static_cast<Hole*>(storage_.many);
        }
    }
public:
    uint16_t group = UINT16_MAX;
    uint32_t size = 0;
    struct Fragments {
        const void* data;
        uint16_t flags;
        size_t size() const { return flags & count_mask; }
        bool empty() const { return size() == 0; }
        Hole operator[](size_t i) const {
            if (flags & compact_bit) {
                const auto& f = static_cast<const SmallFragment*>(data)[i];
                return Hole{f.chunk, f.offset, size_t(f.length_minus_one) + 1};
            }
            return static_cast<const Hole*>(data)[i];
        }
        struct Iterator {
            using iterator_category = std::input_iterator_tag;
            using value_type = Hole;
            using difference_type = std::ptrdiff_t;
            using reference = Hole;
            using pointer = void;
            const void* data; uint16_t flags; size_t index;
            Hole operator*() const { return Fragments{data, flags}[index]; }
            Iterator& operator++() { ++index; return *this; }
            Iterator operator++(int) { auto old = *this; ++*this; return old; }
            bool operator==(const Iterator& other) const { return data == other.data && flags == other.flags && index == other.index; }
            bool operator!=(const Iterator& other) const { return !(*this == other); }
        };
        Iterator begin() const { return {data, flags, 0}; }
        Iterator end() const { return {data, flags, size()}; }
        size_t heap_capacity() const { return size() > (flags & compact_bit ? 4u : 2u) ? size() : 0; }
        const void* heap_data() const { return heap_capacity() ? data : nullptr; }
        size_t record_bytes() const { return flags & compact_bit ? sizeof(SmallFragment) : sizeof(Hole); }
    };
    int node(int k) const { return group == UINT16_MAX ? -1 : group / k; }
    int coefficient(int k) const { return group == UINT16_MAX ? -1 : group % k; }
    Fragments fragments() const {
        const void* data = heap() ? storage_.many : compact() ? static_cast<const void*>(storage_.small.data) : storage_.wide.data;
        return {data, flags_};
    }
    CompactObject() = default;
    ~CompactObject() { release(); }
    CompactObject(const CompactObject& other) : group(other.group), size(other.size) {
        if (other.heap()) {
            if (other.compact()) {
                auto copy = std::make_unique<SmallFragment[]>(other.count());
                auto data = static_cast<const SmallFragment*>(other.storage_.many);
                std::copy(data, data + other.count(), copy.get()); storage_.many = copy.release();
            } else {
                auto copy = std::make_unique<Hole[]>(other.count());
                auto data = static_cast<const Hole*>(other.storage_.many);
                std::copy(data, data + other.count(), copy.get()); storage_.many = copy.release();
            }
        } else if (other.compact()) new (&storage_.small) SmallArray(other.storage_.small);
        else new (&storage_.wide) WideArray(other.storage_.wide);
        flags_ = other.flags_;
    }
    CompactObject(CompactObject&& other) noexcept { *this = std::move(other); }
    CompactObject& operator=(const CompactObject& other) {
        if (this != &other) { CompactObject copy(other); *this = std::move(copy); }
        return *this;
    }
    CompactObject& operator=(CompactObject&& other) noexcept {
        if (this == &other) return *this;
        release();
        if (other.heap()) storage_.many = other.storage_.many;
        else if (other.compact()) new (&storage_.small) SmallArray(other.storage_.small);
        else new (&storage_.wide) WideArray(other.storage_.wide);
        flags_ = other.flags_; group = other.group; size = other.size;
        new (&other.storage_.wide) WideArray(); other.flags_ = 0; other.group = UINT16_MAX; other.size = 0;
        return *this;
    }
    void assign_fragments(const Plan& plan) {
        if (plan.pieces.size() > 4096) throw std::length_error("fragment count exceeds Fmax limit");
        CompactObject replacement;
        replacement.group = group; replacement.size = size;
        auto n = plan.pieces.size();
        bool small = n > 0;
        for (const auto& p : plan.pieces)
            small &= p.hole.chunk <= UINT32_MAX && p.hole.offset <= UINT16_MAX && p.take > 0 && p.take <= 65536;
        if (small) {
            SmallArray local{};
            auto copy = n > 4 ? std::make_unique<SmallFragment[]>(n) : nullptr;
            auto target = copy ? copy.get() : local.data;
            for (size_t i = 0; i < n; ++i) {
                const auto& p = plan.pieces[i];
                target[i] = {static_cast<uint32_t>(p.hole.chunk), static_cast<uint16_t>(p.hole.offset), static_cast<uint16_t>(p.take - 1)};
            }
            if (copy) replacement.storage_.many = copy.release();
            else new (&replacement.storage_.small) SmallArray(local);
        } else {
            WideArray local{};
            auto copy = n > 2 ? std::make_unique<Hole[]>(n) : nullptr;
            auto target = copy ? copy.get() : local.data;
            for (size_t i = 0; i < n; ++i) {
                const auto& p = plan.pieces[i]; target[i] = Hole{p.hole.chunk, p.hole.offset, p.take};
            }
            if (copy) replacement.storage_.many = copy.release();
            else new (&replacement.storage_.wide) WideArray(local);
        }
        replacement.flags_ = static_cast<uint16_t>(n) | (small ? compact_bit : 0);
        *this = std::move(replacement);
    }
};
static_assert(sizeof(CompactObject) == 40, "packed object metadata");
} // namespace ec::mosaic
