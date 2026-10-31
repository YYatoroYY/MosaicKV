#include "inline_fragments.h"
#include "compact_stripes.h"
#include "compact_object.h"
#include "compact_baseline.h"
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <new>
#include <random>

// Inject allocation failures at each allocation site, before or during edits.
static long fail_after = -1;
void* operator new(std::size_t n) {
    if (fail_after == 0) throw std::bad_alloc();
    if (fail_after > 0) --fail_after;
    if (auto p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
__attribute__((noinline)) void operator delete(void* p) noexcept { std::free(p); }
__attribute__((noinline)) void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void* operator new[](std::size_t n) { return ::operator new(n); }
__attribute__((noinline)) void operator delete[](void* p) noexcept { ::operator delete(p); }
__attribute__((noinline)) void operator delete[](void* p, std::size_t) noexcept { ::operator delete(p); }
using namespace ec::mosaic;

template<class Operation> void failure_atomicity(Operation operation) {
    bool completed = false;
    for (int budget = 0; budget < 100 && !completed; ++budget) {
        HoleTree tree;
        tree.add_chunk(1, 256);
        auto plan = tree.allocate(32); assert(plan && tree.commit(*plan));
        auto second = tree.allocate(32); assert(second && tree.commit(*second));
        tree.release({1, 0, 32});
        auto before = tree.holes(); auto bytes = tree.free_bytes();
        auto count = tree.chunk_count(); auto revision = tree.revision();
        // Prepare operation-specific inputs before fault injection.
        auto candidate = tree.allocate(100); assert(candidate);
        fail_after = budget;
        try { operation(tree, *candidate); fail_after = -1; completed = true; }
        catch (const std::bad_alloc&) {
            fail_after = -1;
            assert(tree.holes() == before && tree.free_bytes() == bytes);
            assert(tree.chunk_count() == count && tree.revision() == revision);
        }
    }
    assert(completed);
}
int main() {
    // Compact baseline single-object copies allocate no extent/ID storage.
    ec::StripeId::initialize(std::string(32, 'a'));
    ec::StripeId id(std::string(32, 'a'));
    fail_after = 0;
    ec::InlineExtents one{{id, 0, 12}};
    auto copy_one = one;
    fail_after = -1;
    assert(copy_one.size() == 1 && !copy_one.heap_data());
    ec::InlineExtents many{{id, 0, 12}, {id, 32, 8}};
    fail_after = 0;
    bool copy_failed = false;
    try { one = many; } catch (const std::bad_alloc&) { copy_failed = true; }
    fail_after = -1;
    assert(copy_failed && one.size() == 1 && one[0].length == 12 && !one.heap_data());
    static_assert(sizeof(Hole) == 16);
    static_assert(sizeof(InlineFragments) == 24);
    std::mt19937 random(8714);
    std::vector<InlineFragments> lists(12);
    std::vector<std::vector<Hole>> expected(12);
    for (int i = 0; i < 10000; ++i) {
        auto a = random() % lists.size(), b = random() % lists.size();
        switch (random() % 4) {
        case 0:
            if (expected[a].size() < 32) {
                Hole h{random(), random() % 2048, 1 + random() % 1024};
                lists[a].push_back(h); expected[a].push_back(h);
            }
            break;
        case 1: lists[a] = lists[b]; expected[a] = expected[b]; break;
        case 2:
            if (a != b) { lists[a] = std::move(lists[b]); expected[a] = std::move(expected[b]); expected[b].clear(); }
            break;
        default: lists[a] = InlineFragments{}; expected[a].clear(); break;
        }
        for (size_t j = 0; j < lists.size(); ++j) {
            assert(lists[j].size() == expected[j].size());
            assert(std::equal(lists[j].begin(), lists[j].end(), expected[j].begin()));
            if (lists[j].size() <= 1) assert(lists[j].heap_capacity() == 0);
        }
    }
    failure_atomicity([](HoleTree& t, const Plan&) { t.add_chunk(2, 128); });
    failure_atomicity([](HoleTree& t, const Plan& p) { assert(t.commit(p)); });
    failure_atomicity([](HoleTree& t, const Plan&) { t.release({1, 32, 32}); });
    HoleTree tree; tree.add_chunk(0, 256);
    auto p = tree.allocate(64); assert(p && tree.commit(*p));
    for (Hole bad : {Hole{1, 0, 64}, {0, 32, 64}, {0, 64, 1}, {0, 255, 2}, {0, 0, 0}}) {
        auto before = tree.holes(); bool rejected = false;
        try { tree.release(bad); } catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected && tree.holes() == before);
    }
    tree.release({0, 0, 64});
    bool rejected = false;
    try { tree.release({0, 0, 64}); } catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected && tree.free_bytes() == 256);
    // Both intrusive indices must transfer ownership together, including when
    // move assignment replaces an already populated tree.
    HoleTree source; source.add_chunk(9, 128);
    auto owned = source.allocate(32); assert(owned && source.commit(*owned));
    HoleTree moved(std::move(source));
    assert(source.chunk_count() == 0 && source.free_bytes() == 0);
    HoleTree target; target.add_chunk(4, 512);
    target = std::move(moved);
    assert(moved.chunk_count() == 0 && moved.hole_count() == 0);
    target.release({9, 0, 32});
    assert(target.chunk_count() == 1 && target.free_bytes() == 128);
    // Packed objects own exact-sized fragment arrays. Exercise transitions
    // between zero/inline/multiple fragments and binary node/class packing.
    CompactObject packed;
    for (int i = 0; i < 4096; ++i) {
        Plan p{0, 0, {}};
        auto count = random() % 9;
        for (size_t j = 0; j < count; ++j) p.pieces.push_back({Hole{j, j * 7, 32}, 17});
        packed.group = static_cast<uint16_t>(random() % (256 * 31)); packed.size = 17 * count;
        packed.assign_fragments(p);
        auto copy = packed;
        assert(copy.size == packed.size && copy.group == packed.group);
        assert(copy.node(31) * 31 + copy.coefficient(31) == copy.group);
        for (size_t j = 0; j < count; ++j) assert(copy.fragments()[j] == packed.fragments()[j]);
        auto moved = std::move(copy); assert(copy.fragments().empty());
        packed = moved; packed = std::move(moved);
        assert(packed.fragments().size() == count);
        assert(packed.fragments().heap_capacity() == (count > 4 ? count : 0));
    }
    Plan maximum{0, 0, {}};
    for (size_t i = 0; i < 4096; ++i) maximum.pieces.push_back({Hole{i, 0, 1048576}, 1});
    packed.assign_fragments(maximum);
    assert(packed.fragments().size() == 4096 && packed.fragments()[4095].chunk == 4095);
    auto saved_size = packed.fragments().size();
    fail_after = 0;
    bool out_of_memory = false;
    try { packed.assign_fragments(maximum); }
    catch (const std::bad_alloc&) { out_of_memory = true; }
    fail_after = -1;
    assert(out_of_memory && packed.fragments().size() == saved_size && packed.fragments()[4095].chunk == 4095);
    assert(packed.fragments().record_bytes() == 8);
    Plan wide{0, 0, {{{UINT64_C(1) << 40, 70000, 100000}, 100000}, {{1, 0, 65536}, 65536}}};
    packed.assign_fragments(wide);
    assert(packed.fragments().record_bytes() == 16);
    assert(packed.fragments()[0] == (Hole{UINT64_C(1) << 40, 70000, 100000}));
    assert(packed.fragments()[1].length == 65536);
    auto wide_copy = packed;
    packed.assign_fragments(maximum);
    assert(wide_copy.fragments()[0].chunk == (UINT64_C(1) << 40));
    Plan boundary{0, 0, {{{UINT32_MAX, UINT16_MAX, 65536}, 65536}, {{0, 0, 1}, 1}}};
    packed.assign_fragments(boundary);
    assert(packed.fragments().record_bytes() == 8 && packed.fragments()[0].length == 65536);
    // Catalog lookups survive page boundaries and failed ID gaps. References
    // to committed records remain valid while new pages are allocated.
    CompactStripes usage;
    usage.prepare({42, 0}); usage.commit();
    assert(usage.note_write(42, 0) == 3);
    assert(usage.note_write(42, 0) == 0);
    assert(usage.note_write(42, 30) == 1);
    assert(usage.at(42).written_mask == ((uint32_t(1) << 30) | 1));
    CompactStripes catalog;
    const CompactStripes::Stripe* first = nullptr;
    for (uint64_t i = 0; i < 1800; ++i) {
        catalog.prepare({i * 3, static_cast<uint16_t>(i % 256)});
        if (i % 7 == 0) { catalog.cancel(); continue; }
        catalog.commit();
        if (!first) first = &catalog.at(i * 3);
        assert(first->id == 3 && first->start == 1);
    }
    for (uint64_t i = 0; i < 1800; ++i) {
        bool missing = false;
        try { assert(catalog.at(i * 3).start == i % 256); }
        catch (const std::out_of_range&) { missing = true; }
        assert(missing == (i % 7 == 0));
    }
    auto entries = catalog.size(), memory = catalog.metadata_bytes();
    for (uint64_t i = 1800; i < 11800; ++i) { catalog.prepare({i * 3, 0}); catalog.cancel(); }
    assert(catalog.size() == entries && catalog.metadata_bytes() == memory);
    for (int budget = 0; budget < 4; ++budget) {
        CompactStripes empty;
        fail_after = budget;
        try { empty.prepare({0, 0}); fail_after = -1; empty.cancel(); }
        catch (const std::bad_alloc&) { fail_after = -1; empty.cancel(); }
        assert(empty.size() == 0);
        empty.prepare({9, 2}); empty.commit(); assert(empty.at(9).start == 2);
    }
    std::cout << "PASS compact metadata: 10000 fragment ownership mutations, allocation-failure atomicity, range/overlap/double-free checks\n";
}
