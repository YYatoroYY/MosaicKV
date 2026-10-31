#include "compact_baseline.h"
#include <cassert>
#include <iostream>
#include <random>
#include <sstream>
using namespace ec;
int main() {
    StripeId::initialize(std::string(32, 'a'));
    std::mt19937 rng(257);
    StripeId previous;
    for (int i = 0; i < 10000; ++i) {
        std::string text(32, '0');
        for (size_t j = 0; j < 16; ++j) text[j] = 'a';
        for (size_t j = 16; j < 32; ++j) text[j] = "0123456789abcdef"[rng() % 16];
        StripeId id(text);
        assert(std::string(id) == text);
        assert((previous < id) == (std::string(previous) < text));
        std::stringstream stream; stream << id; StripeId decoded; stream >> decoded;
        assert(stream && decoded == id); previous = id;
    }
    for (const char* invalid : {"short", "-", "", "g0123456789abcdef0123456789abcdef", "bbbbbbbbbbbbbbbb0000000000000001"}) {
        std::istringstream stream(invalid); StripeId id; stream >> id; assert(!stream);
    }
    std::vector<InlineExtents> lists(16);
    std::vector<std::vector<CompactExtent>> expected(16);
    auto equal = [](const auto& a, const auto& b) { return a.stripe == b.stripe && a.offset == b.offset && a.length == b.length; };
    for (int i = 0; i < 10000; ++i) {
        auto a = rng() % lists.size(), b = rng() % lists.size();
        switch (rng() % 5) {
        case 0:
            if (expected[a].size() < 32) { CompactExtent e{previous, rng() % (UINT32_MAX-1), rng()}; lists[a].push_back(e); expected[a].push_back(e); }
            break;
        case 1: lists[a] = lists[b]; expected[a] = expected[b]; break;
        case 2:
            if (a != b) { lists[a] = std::move(lists[b]); expected[a] = std::move(expected[b]); expected[b].clear(); }
            break;
        case 3: lists[a].clear(); expected[a].clear(); break;
        default: lists[a] = InlineExtents{{previous, 0, UINT32_MAX}}; expected[a] = {{previous, 0, UINT32_MAX}}; break;
        }
        for (size_t j = 0; j < lists.size(); ++j) {
            assert(lists[j].size() == expected[j].size());
            assert(std::equal(lists[j].begin(), lists[j].end(), expected[j].begin(), equal));
        }
    }
    InlineExtents single{{previous, UINT32_MAX-2, UINT32_MAX}};
    assert(!single.heap_data());
    bool rejected = false;
    try { CompactExtent bad{previous, uint64_t(UINT32_MAX) + 1, 0}; (void)bad; }
    catch (const std::length_error&) { rejected = true; }
    assert(rejected);
    for (uint64_t i = 1; i <= 1000; ++i) assert(StripeId::next().value == i);
    StripeId::sequence = UINT64_MAX;
    rejected = false;
    try { (void)StripeId::next(); } catch (const std::overflow_error&) { rejected = true; }
    assert(rejected); StripeId::sequence = 1000;
    // Non-GC methods maintain exact liveness accounting without candidate trees.
    struct Stripe { int start = 0, data_blocks = 3; std::vector<uint32_t> lengths{1, 17, 0}; uint32_t live_mask = 5; } stripe;
    StripeAccounting account(5, 2);
    account.account(stripe, true);
    assert(account.total == 18 && account.invalid == 17 && account.parity == 34 && account.logical == 0 && account.dirty_count == 1);
    assert((account.node_bytes == std::vector<uint64_t>{1, 17, 0, 17, 17}));
    account.account(stripe, false);
    assert(account.total == 0 && account.invalid == 0 && account.parity == 0 && account.logical == 0 && account.dirty_count == 0);
    StripeAccounting::Edit edit;
    edit.affected.insert(previous); edit.entries.emplace(previous, StripeAccounting::Entry{3, 64, 5});
    edit.prepare(false); account.publish_candidates(edit); assert(account.dirty.empty());
    edit.prepare(true); account.publish_candidates(edit); assert(account.dirty.size() == 1);
    edit.entries.at(previous).mask = 0; edit.prepare(true); account.publish_candidates(edit);
    assert(account.dirty.empty() && account.empty.size() == 1);
    std::cout << "PASS compact baseline UUID, inline ownership, accounting and candidates\n";
}
