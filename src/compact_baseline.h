#pragma once
#include <algorithm>
#include <array>
#include <cstdint>
#include <initializer_list>
#include <istream>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace ec {
// A process-local monotonically increasing ID plus one shared random namespace.
// Wire names stay 32 lowercase hex digits. No retained interning dictionary and
// no per-reference UUID prefix. This RAM store does not import foreign catalogs.
struct StripeId {
    uint64_t value = 0;
    inline static uint64_t prefix = 0, sequence = 0;
    StripeId() = default;
    explicit StripeId(uint64_t id) : value(id) {}
    static void initialize(const std::string& seed) {
        if (sequence) throw std::logic_error("stripe namespace already in use");
        prefix = std::stoull(seed.substr(0, 16), nullptr, 16);
    }
    static StripeId next() {
        if (sequence == UINT64_MAX) throw std::overflow_error("stripe ID exhausted");
        return StripeId(++sequence); // Store mutex serializes foreground and GC.
    }
    StripeId(const std::string& text) {
        if (text.size() != 32 || text.find_first_not_of("0123456789abcdef") != std::string::npos)
            throw std::invalid_argument("invalid stripe ID");
        if (std::stoull(text.substr(0, 16), nullptr, 16) != prefix)
            throw std::invalid_argument("foreign stripe namespace");
        value = std::stoull(text.substr(16), nullptr, 16);
    }
    operator std::string() const {
        static constexpr char hex[] = "0123456789abcdef";
        std::string text(32, '0');
        for (int i = 0; i < 16; ++i) {
            text[15-i] = hex[(prefix >> (4*i)) & 15];
            text[31-i] = hex[(value >> (4*i)) & 15];
        }
        return text;
    }
    friend bool operator<(StripeId a, StripeId b) { return a.value < b.value; }
    friend bool operator==(StripeId a, StripeId b) { return a.value == b.value; }
    friend bool operator!=(StripeId a, StripeId b) { return !(a == b); }
    friend std::string operator+(StripeId a, const std::string& b) { return std::string(a) + b; }
    friend std::string operator+(const std::string& a, StripeId b) { return a + std::string(b); }
    friend std::ostream& operator<<(std::ostream& out, StripeId id) { return out << std::string(id); }
    friend std::istream& operator>>(std::istream& in, StripeId& id) {
        std::string text;
        if (in >> text) {
            try { id = StripeId(text); }
            catch (const std::invalid_argument&) { in.setstate(std::ios::failbit); }
        }
        return in;
    }
};
static_assert(sizeof(StripeId) == 8);

struct CompactExtent {
    StripeId stripe;
    uint32_t offset = 0, length = 0;
    CompactExtent() = default;
    CompactExtent(StripeId id, size_t off, size_t len) : stripe(id) {
        // Top two values are private inline-container tags. Valid EC positions
        // are < 32 * MAX_BLOCK and never approach these values.
        if (off >= UINT32_MAX - 1 || len > UINT32_MAX) throw std::length_error("extent exceeds compact range");
        offset = off; length = len;
    }
};
static_assert(sizeof(CompactExtent) == 16);

// The common single extent IS the entire object record. Empty/multiple extent
// tags reuse impossible offset values, so there is no active-count or duplicate
// logical-length field for object layout. Only legacy block layout allocates a
// vector control block; its logical size is derived from its extents.
class InlineExtents {
    static constexpr uint32_t empty_tag = UINT32_MAX, heap_tag = UINT32_MAX - 1;
    CompactExtent single_;
    bool heap() const { return single_.offset == heap_tag; }
    std::vector<CompactExtent>* many() { return reinterpret_cast<std::vector<CompactExtent>*>(single_.stripe.value); }
    const std::vector<CompactExtent>* many() const { return reinterpret_cast<const std::vector<CompactExtent>*>(single_.stripe.value); }
public:
    InlineExtents() { single_.offset = empty_tag; }
    InlineExtents(std::initializer_list<CompactExtent> values) : InlineExtents() { for (auto v : values) push_back(v); }
    ~InlineExtents() { if (heap()) delete many(); }
    InlineExtents(const InlineExtents& other) : InlineExtents() {
        if (other.heap()) {
            auto copy = std::make_unique<std::vector<CompactExtent>>(*other.many());
            single_.stripe.value = reinterpret_cast<uintptr_t>(copy.release()); single_.offset = heap_tag;
        } else single_ = other.single_;
    }
    InlineExtents(InlineExtents&& other) noexcept : single_(other.single_) { other.single_ = {}; other.single_.offset = empty_tag; }
    InlineExtents& operator=(const InlineExtents& other) {
        if (this != &other) { InlineExtents copy(other); *this = std::move(copy); }
        return *this;
    }
    InlineExtents& operator=(InlineExtents&& other) noexcept {
        if (this != &other) { clear(); single_ = other.single_; other.single_ = {}; other.single_.offset = empty_tag; }
        return *this;
    }
    void reserve(size_t capacity) {
        if (capacity <= 1 || (heap() && capacity <= many()->capacity())) return;
        if (capacity > UINT32_MAX) throw std::length_error("extent count exceeds uint32");
        if (heap()) { many()->reserve(capacity); return; }
        auto copy = std::make_unique<std::vector<CompactExtent>>(); copy->reserve(capacity);
        if (!empty()) copy->push_back(single_);
        single_ = {}; single_.stripe.value = reinterpret_cast<uintptr_t>(copy.release()); single_.offset = heap_tag;
    }
    void push_back(CompactExtent value) {
        if (value.offset >= heap_tag) throw std::length_error("reserved extent offset");
        if (empty() && !heap()) { single_ = value; return; }
        if (!heap()) reserve(2);
        many()->push_back(value);
    }
    void clear() noexcept { if (heap()) delete many(); single_ = {}; single_.offset = empty_tag; }
    const void* heap_data() const { return heap() ? many()->data() : nullptr; }
    const void* heap_control_data() const { return heap() ? many() : nullptr; }
    size_t heap_capacity() const { return heap() ? many()->capacity() : 0; }
    size_t size() const { return heap() ? many()->size() : single_.offset == empty_tag ? 0 : 1; }
    bool empty() const { return size() == 0; }
    CompactExtent* begin() { return heap() ? many()->data() : &single_; }
    CompactExtent* end() { return begin() + size(); }
    const CompactExtent* begin() const { return heap() ? many()->data() : &single_; }
    const CompactExtent* end() const { return begin() + size(); }
    CompactExtent& operator[](size_t i) { return begin()[i]; }
    const CompactExtent& operator[](size_t i) const { return begin()[i]; }
    CompactExtent& at(size_t i) { if (i >= size()) throw std::out_of_range("extent index"); return begin()[i]; }
    const CompactExtent& at(size_t i) const { if (i >= size()) throw std::out_of_range("extent index"); return begin()[i]; }
    CompactExtent& back() { return at(size() - 1); }
};
static_assert(sizeof(InlineExtents) == 16);

// Compile-time specialization removes the unused epoch from non-GC records.
// The empty specialization's operations are no-ops; it retains no epoch value.
template<bool Enabled> struct StripeEpoch;
template<> struct StripeEpoch<true> {
    uint64_t value = 0;
    operator uint64_t() const { return value; }
    StripeEpoch& operator=(uint64_t n) { value = n; return *this; }
    StripeEpoch& operator++() { ++value; return *this; }
};
template<> struct StripeEpoch<false> {
    operator uint64_t() const { return 0; }
    StripeEpoch& operator=(uint64_t) { return *this; }
    StripeEpoch& operator++() { return *this; }
};

// Live masks reside in the stripe directory, alongside its sole length array.
// Global logical bytes are maintained incrementally from affected objects.
// Only FastGC has persistent candidate sets. Scalar and per-node accounting is maintained for every method without duplicating the directory.
class StripeAccounting {
public:
    struct Entry {
        int data = 0, bytes = 0;
        uint32_t mask = 0;
    };
    struct Edit {
        std::set<StripeId> affected, dirty, empty;
        std::map<StripeId, Entry> entries; // Request-local preparation only.
        void prepare(bool track_candidates) {
            if (!track_candidates) return;
            for (const auto& [id, e] : entries) {
                if (!e.mask) empty.insert(id);
                else if (__builtin_popcount(e.mask) < e.data) dirty.insert(id);
            }
        }
    };
    std::set<StripeId> dirty, empty;
    uint64_t total = 0, invalid = 0, parity = 0, logical = 0, dirty_count = 0;
    std::vector<uint64_t> node_bytes;
    int m = 0;
    StripeAccounting(size_t nodes = 0, int parities = 0) : node_bytes(nodes), m(parities) {}
    template<class Stripe> void account(const Stripe& s, bool add) noexcept {
        auto change = [add](uint64_t& target, uint64_t amount) { if (add) target += amount; else target -= amount; };
        auto parity_bytes = *std::max_element(s.lengths.begin(), s.lengths.end());
        for (int i = 0; i < s.data_blocks; ++i) {
            change(total, s.lengths[i]);
            if (!(s.live_mask & (uint32_t(1) << i))) change(invalid, s.lengths[i]);
        }
        change(parity, uint64_t(m) * parity_bytes);
        change(dirty_count, __builtin_popcount(s.live_mask) < s.data_blocks);
        for (int i = 0; i < s.data_blocks + m; ++i)
            change(node_bytes[(s.start + i) % node_bytes.size()], i < s.data_blocks ? s.lengths[i] : parity_bytes);
    }
    void publish_candidates(Edit& edit) noexcept {
        for (const auto& id : edit.affected) { dirty.erase(id); empty.erase(id); }
        dirty.merge(edit.dirty); empty.merge(edit.empty);
    }
};
} // namespace ec
