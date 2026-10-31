#pragma once
#include "metadata_memory.h"
#include <array>
#include <memory>
#include <stdexcept>
#include <vector>

namespace ec::mosaic {
// Append-only committed stripes, held in fixed pages. Failed attempts do not
// consume slots; monotonically increasing IDs may have gaps. A page allocation
// is prepared before I/O, and publication never allocates or relocates records.
class CompactStripes {
public:
    struct Stripe {
        uint64_t id = 0; uint16_t start = 0;
        uint32_t written_mask = 0; // Diagnostic only; never used for storage/placement decisions.
        uint32_t materialized_mask = 0; // Data shards absent from this mask are virtual zeroes.
    };
private:
    static constexpr size_t per_page = 256;
    using Page = std::array<Stripe, per_page>;
    std::vector<std::unique_ptr<Page>> pages_;
    size_t size_ = 0;
    bool prepared_ = false;
    Stripe& slot(size_t index) { return (*pages_[index / per_page])[index % per_page]; }
public:
    size_t size() const { return size_; }
    const Stripe& operator[](size_t index) const { return (*pages_[index / per_page])[index % per_page]; }
    const Stripe& at(uint64_t id) const {
        size_t first = 0, last = size_;
        while (first < last) {
            auto mid = first + (last - first) / 2;
            if ((*this)[mid].id < id) first = mid + 1; else last = mid;
        }
        if (first == size_ || (*this)[first].id != id) throw std::out_of_range("unknown chunk ID");
        return (*this)[first];
    }
    // Caller holds the Store lock. Track the first write to each data chunk
    // and stripe for space accounting.
    unsigned note_write(uint64_t id, unsigned index) {
        if (index >= 31) throw std::out_of_range("invalid data coefficient");
        auto& stripe = const_cast<Stripe&>(at(id));
        const uint32_t bit = uint32_t(1) << index;
        if (stripe.written_mask & bit) return 0;
        unsigned changed = stripe.written_mask ? 1 : 3;
        stripe.written_mask |= bit;
        return changed;
    }
    void materialize(uint64_t id, unsigned index) {
        if (index >= 31) throw std::out_of_range("invalid data coefficient");
        const_cast<Stripe&>(at(id)).materialized_mask |= uint32_t(1) << index;
    }
    void prepare(Stripe stripe) {
        if (prepared_ || (size_ && stripe.id <= (*this)[size_ - 1].id))
            throw std::invalid_argument("nonmonotonic stripe publication");
        if (size_ / per_page == pages_.size()) pages_.push_back(std::make_unique<Page>());
        slot(size_) = stripe;
        prepared_ = true;
    }
    void commit() noexcept {
        if (!prepared_) std::terminate();
        ++size_; prepared_ = false;
    }
    void cancel() noexcept {
        prepared_ = false;
        if (!size_ || size_ % per_page == 0) {
            if (pages_.size() > size_ / per_page) pages_.pop_back();
        }
    }
    uint64_t metadata_bytes() const {
        uint64_t total = metadata_memory::vector(pages_);
        for (const auto& page : pages_) total += metadata_memory::allocation(page.get());
        return total;
    }
};
} // namespace ec::mosaic
