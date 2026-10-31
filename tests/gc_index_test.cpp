#include "gc_index.h"
#include <cassert>
#include <iostream>
#include <random>
using namespace ec;
int main() {
    GcAmplificationPeak amplification;
    amplification.observe(0, 0, 0, 0);
    assert(amplification.live == 0 && amplification.normalized(6, 3) == 0);
    amplification.observe(100, 50, 0, 100);
    assert(amplification.normalized(6, 3) == 1);
    amplification.observe(300, 140, 10, 100);
    amplification.observe(700, 300, 0, 500); // Largest payload is not largest ratio.
    assert(amplification.payload == 450 && amplification.live == 100);
    amplification.observe(200, 70, 30, 50); // Smaller denominator raises the ratio.
    amplification.observe(50, 25, 0, 50); // GC must not erase the historical peak.
    assert(amplification.normalized(6, 3) == 4 && amplification.payload == 300);
    assert(amplification.data == 200 && amplification.parity == 70 && amplification.replicas == 30);
    assert(amplification.observation_at_peak == 5 && amplification.observations == 6);
    amplification = {};
    amplification.observe(uint64_t(1) << 63, 0, 0, uint64_t(1) << 62);
    amplification.observe((uint64_t(1) << 63) + 1, 0, 0, uint64_t(1) << 62);
    assert(amplification.payload == (uint64_t(1) << 63) + 1); // Overflow-safe exact comparison.
    amplification = {};
    amplification.observe(100, 50, 0, 100);
    assert(amplification.observations == 1 && amplification.normalized(6, 3) == 1);
    GcSpacePeaks peaks;
    peaks.observe(1000, 0, 200, 220); // Padding peak, with additional live buffer bytes.
    peaks.observe(1000, 700, 200, 230); // Invalid peak at a different time.
    peaks.observe(200, 0, 200, 200); // GC removes waste; history must survive.
    assert(peaks.padding == 800 && peaks.invalid == 700 && peaks.waste == 800);
    assert(peaks.live_at_padding == 220 && peaks.observations == 3);
    peaks = {};
    peaks.observe(200, 0, 200, 200);
    assert(peaks.padding == 0 && peaks.invalid == 0 && peaks.waste == 0 && peaks.observations == 1);
    GcIndex index(9, 3);
    std::map<std::string, GcIndex::Entry> reference;
    std::mt19937 rng(937);
    for (int step = 0; step < 20000; ++step) {
        GcIndex::Edit edit;
        auto next = reference;
        for (int n = 0; n < 1 + step % 7; ++n) {
            auto id = std::to_string(rng() % 97);
            edit.affected.insert(id);
            edit.entries.erase(id); next.erase(id);
            if (rng() % 4) {
                int data = 1 + rng() % 6, bytes = 64 + rng() % 449;
                uint32_t mask = rng() & ((1u << data) - 1);
                // Live zero-length slots count toward liveness but not bytes.
                uint64_t logical = __builtin_popcount(mask) * (rng() % (bytes + 1));
                GcIndex::Entry entry{int(rng() % 9), data, bytes, mask, logical};
                if (step % 2) {
                    entry.logical = 0;
                    for (int i = 0; i < data; ++i) {
                        size_t length = rng() % (bytes + 1); entry.lengths.push_back(length);
                        if (mask & (1u << i)) entry.logical += length;
                    }
                }
                next[id] = edit.entries[id] = entry;
            }
        }
        edit.prepare();
        if (step % 11) { index.apply(std::move(edit)); reference = std::move(next); }
        uint64_t total = 0, invalid = 0, parity = 0, logical = 0;
        std::vector<uint64_t> nodes(9);
        std::set<std::string> dirty, empty;
        for (const auto& [id, e] : reference) {
            size_t parity_size = e.lengths.empty() ? e.bytes : *std::max_element(e.lengths.begin(), e.lengths.end());
            for (int i = 0; i < e.data; ++i) {
                auto size = e.lengths.empty() ? e.bytes : e.lengths[i];
                total += size; if (!(e.mask & (1u << i))) invalid += size;
            }
            parity += 3 * parity_size; logical += e.logical;
            if (!e.mask) empty.insert(id);
            else if (__builtin_popcount(e.mask) < e.data) dirty.insert(id);
            for (int j = 0; j < e.data + 3; ++j) nodes[(e.start + j) % 9] += j < e.data ? (e.lengths.empty() ? e.bytes : e.lengths[j]) : parity_size;
        }
        assert(index.entries.size() == reference.size());
        assert(index.total == total && index.invalid == invalid && index.parity == parity && index.logical == logical);
        assert(index.dirty == dirty && index.empty == empty && index.node_bytes == nodes);
    }
    std::cout << "PASS incremental GC index: 20000 randomized publications/aborts, masks, totals and placements\n";
}
