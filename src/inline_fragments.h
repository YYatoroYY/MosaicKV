#pragma once
#include "mosaic_allocator.h"
#include <memory>
#include <new>

namespace ec::mosaic {
// One descriptor occupies the same storage as the heap pointer. Empty/single
// fragment objects allocate nothing; multi-fragment objects own a small array.
class InlineFragments {
    union Storage {
        Hole single;
        Hole* many;
        Storage() : single{} {}
    } storage_;
    uint32_t size_ = 0, capacity_ = 1;
    Hole* data() { return capacity_ == 1 ? &storage_.single : storage_.many; }
    const Hole* data() const { return capacity_ == 1 ? &storage_.single : storage_.many; }
public:
    InlineFragments() = default;
    ~InlineFragments() { if (capacity_ > 1) delete[] storage_.many; }
    InlineFragments(const InlineFragments& other) {
        if (other.size_ > 1) {
            auto copy = std::make_unique<Hole[]>(other.size_);
            std::copy(other.begin(), other.end(), copy.get());
            storage_.many = copy.release(); capacity_ = other.size_;
        } else if (other.size_) storage_.single = other.storage_.single;
        size_ = other.size_;
    }
    InlineFragments(InlineFragments&& other) noexcept { *this = std::move(other); }
    InlineFragments& operator=(const InlineFragments& other) {
        if (this != &other) { InlineFragments copy(other); *this = std::move(copy); }
        return *this;
    }
    InlineFragments& operator=(InlineFragments&& other) noexcept {
        if (this == &other) return *this;
        if (capacity_ > 1) delete[] storage_.many;
        if (other.capacity_ > 1) storage_.many = other.storage_.many;
        else new (&storage_.single) Hole(other.storage_.single);
        size_ = other.size_; capacity_ = other.capacity_;
        new (&other.storage_.single) Hole();
        other.size_ = 0; other.capacity_ = 1;
        return *this;
    }
    void reserve(size_t capacity) {
        if (capacity <= capacity_) return;
        if (capacity > UINT32_MAX) throw std::length_error("fragment capacity exceeds uint32");
        auto copy = std::make_unique<Hole[]>(capacity);
        std::copy(begin(), end(), copy.get());
        if (capacity_ > 1) delete[] storage_.many;
        storage_.many = copy.release(); capacity_ = static_cast<uint32_t>(capacity);
    }
    void push_back(Hole value) {
        if (size_ == capacity_) reserve(size_t(capacity_) * 2);
        data()[size_++] = value;
    }
    const void* heap_data() const { return capacity_ > 1 ? storage_.many : nullptr; }
    size_t size() const { return size_; }
    bool empty() const { return !size_; }
    size_t heap_capacity() const { return capacity_ > 1 ? capacity_ : 0; }
    const Hole* begin() const { return data(); }
    const Hole* end() const { return data() + size_; }
    const Hole& operator[](size_t i) const { return data()[i]; }
};
static_assert(sizeof(Hole) == 16, "compact fragment layout");
} // namespace ec::mosaic
