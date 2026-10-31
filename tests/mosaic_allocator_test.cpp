#include "mosaic_allocator.h"
#include <cassert>
#include <iostream>
#include <random>
using namespace ec::mosaic;

// Independent vector implementation of the supplied pseudocode.
std::optional<std::vector<Piece>> reference(std::vector<Hole> holes, size_t length, size_t budget, size_t tau) {
    std::sort(holes.begin(), holes.end(), ByLength{});
    std::vector<Piece> result;
    while (length) {
        if (!budget) return std::nullopt;
        auto exact = std::find_if(holes.begin(), holes.end(), [&](const Hole& h) { return h.length == length; });
        if (exact != holes.end()) { result.push_back({*exact, length}); return result; }
        auto chosen = std::find_if(holes.begin(), holes.end(), [&](const Hole& h) { return h.length >= (length + budget - 1) / budget; });
        if (chosen == holes.end()) return std::nullopt;
        size_t difference = chosen->length > length ? chosen->length - length : length - chosen->length;
        if (difference && difference < tau) {
            auto alternative = std::find_if(holes.begin(), holes.end(), [&](const Hole& h) { return h.length >= length + tau; });
            if (alternative != holes.end()) chosen = alternative;
        }
        size_t take = std::min<size_t>(chosen->length, length);
        result.push_back({*chosen, take});
        holes.erase(chosen); length -= take; --budget;
    }
    return result;
}
int main() {
    Parameters defaults;
    defaults.validate();
    assert(defaults.alignment_chunk == 4096 && defaults.fmax == 4 && defaults.tau == 8);
    assert(defaults.fits(16384) && !defaults.fits(16385));
    // Large alignment does not imply one contiguous placement: reuse four holes.
    Parameters large{12 * 1024 * 1024, 4, 8};
    large.validate();
    bool too_large = false;
    try { Parameters{large.alignment_chunk + 1, 4, 8}.validate(); }
    catch (const std::invalid_argument&) { too_large = true; }
    assert(too_large);
    HoleTree fragmented;
    for (uint64_t id = 0; id < 4; ++id) {
        fragmented.add_chunk(id, large.alignment_chunk);
        auto fill = fragmented.allocate(large.alignment_chunk, 1);
        assert(fill && fragmented.commit(*fill));
        fragmented.release({id, 0, 150 * 1024});
    }
    auto four = fragmented.allocate(600 * 1024, 4);
    assert(four && four->pieces.size() == 4 && fragmented.commit(*four));
    assert(fragmented.free_bytes() == 0);
    HoleTree exact;
    {
        HoleTree rollback;
        rollback.add_chunk(7, 128);
        auto plan = rollback.allocate(64);
        assert(plan && rollback.commit(*plan));
        assert(!rollback.remove_empty_chunk(7));
        rollback.release({7, 0, 64});
        assert(rollback.remove_empty_chunk(7));
        assert(!rollback.remove_empty_chunk(7));
        assert(rollback.chunk_count() == 0 && rollback.hole_count() == 0 && rollback.free_bytes() == 0);
        rollback.add_chunk(7, 128);
        assert(!rollback.commit(*plan)); // Rollback/re-add also invalidates stale plans.
    }
    exact.add_chunk(1, 25); exact.add_chunk(2, 100); exact.add_chunk(3, 100);
    auto p = exact.allocate(100);
    assert(p && p->pieces.size() == 1 && p->pieces[0].hole.chunk == 2);
    auto stale = *p;
    assert(exact.commit(*p));
    assert(!exact.commit(stale));
    exact.release({2, 0, 100});
    assert(!exact.commit(stale)); // Even after identical address/length become free again.
    bool rejected = false;
    try { exact.release({2, 0, 100}); } catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);
    for (size_t small : {95, 105}) {
        HoleTree tails;
        tails.add_chunk(1, small); tails.add_chunk(2, 108);
        p = tails.allocate(100, 4, 8);
        assert(p && p->pieces.size() == 1 && p->pieces[0].hole.length == 108);
        p = tails.allocate(100, 4, 0);
        assert(p && p->pieces[0].hole.length == small);
    }
    HoleTree tiny;
    tiny.add_chunk(1, 105);
    p = tiny.allocate(100, 4, 8);
    assert(p && tiny.commit(*p) && tiny.free_bytes() == 5); // tau is a preference, not a hard ban.
    tiny.release({1, 0, 100});
    assert(tiny.hole_count() == 1 && tiny.free_bytes() == 105);
    HoleTree failed;
    failed.add_chunk(0, 30); failed.add_chunk(1, 30);
    auto before = failed.holes();
    assert(!failed.allocate(100, 4)); // Fails after choosing a first hole in temporary V.
    assert(before == failed.holes() && failed.free_bytes() == 60);
    p = failed.allocate(0);
    assert(p && p->pieces.empty() && failed.commit(*p));

    // Random allocations/releases: compare every choice to the reference and
    // track every byte independently to detect overlap, leaks, or bad merging.
    HoleTree tree;
    constexpr size_t chunks = 16, bytes = 256;
    std::vector<std::vector<bool>> occupied(chunks, std::vector<bool>(bytes));
    std::vector<Hole> live;
    for (size_t c = 0; c < chunks; ++c) tree.add_chunk(c, bytes);
    std::mt19937 random(91832);
    for (int step = 0; step < 20000; ++step) {
        if (!live.empty() && random() % 3 == 0) {
            auto index = random() % live.size();
            auto h = live[index];
            tree.release(h);
            for (size_t i = h.offset; i < h.offset + h.length; ++i) { assert(occupied[h.chunk][i]); occupied[h.chunk][i] = false; }
            live.erase(live.begin() + index);
        } else {
            size_t length = random() % 1025, budget = 1 + random() % 4, tau = random() % 17;
            auto expected = reference(tree.holes(), length, budget, tau);
            auto actual = tree.allocate(length, budget, tau);
            assert(bool(expected) == bool(actual));
            if (actual) {
                assert(actual->pieces.size() == expected->size() && actual->pieces.size() <= budget);
                for (size_t i = 0; i < expected->size(); ++i)
                    assert(actual->pieces[i].hole == (*expected)[i].hole && actual->pieces[i].take == (*expected)[i].take);
                assert(tree.commit(*actual));
                for (const auto& piece : actual->pieces) {
                    Hole h{piece.hole.chunk, piece.hole.offset, piece.take};
                    assert(h.offset + h.length <= bytes);
                    for (size_t i = h.offset; i < h.offset + h.length; ++i) { assert(!occupied[h.chunk][i]); occupied[h.chunk][i] = true; }
                    live.push_back(h);
                }
            }
        }
        size_t used = 0;
        for (const auto& chunk : occupied) used += std::count(chunk.begin(), chunk.end(), true);
        assert(used + tree.free_bytes() == chunks * bytes);
        for (const auto& h : tree.holes())
            for (size_t i = h.offset; i < h.offset + h.length; ++i) assert(!occupied[h.chunk][i]);
    }
    for (const auto& h : live) tree.release(h);
    assert(tree.free_bytes() == chunks * bytes && tree.hole_count() == chunks);
    for (auto config : {std::make_tuple(6, 3, 9), {6, 4, 10}, {10, 4, 14}, {10, 6, 16}, {3, 2, 9}}) {
        auto [k, m, nodes] = config;
        Layout layout(k, m, nodes);
        std::vector<std::vector<int>> seen(nodes, std::vector<int>(k + m));
        for (int start = 0; start < nodes; ++start) {
            std::set<int> machines;
            for (int index = 0; index < k + m; ++index) {
                int node = layout.node(start, index);
                assert(machines.insert(node).second);
                ++seen[node][index];
                if (index < k) assert(layout.start_for(node, index) == start);
            }
        }
        for (const auto& node : seen) for (int count : node) assert(count == 1);
    }
    std::cout << "PASS Mosaic: exact pseudocode, tau, failure atomicity, stale plans, 20000 randomized mutations, cyclic layout\n";
}
