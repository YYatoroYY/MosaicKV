#include "native_store.h"
#include "frame_scratch.h"
#include <cassert>
#include <random>
using namespace ec;
void reused_large_ranges() {
    setenv("MOSAIC_BATCH_IO", "1", 1);
    setenv("NATIVE_MEMORY_MB", "128", 1);
    NativeStore store;
    FrameScratch scratch(MAX_VALUE);
    // Max legal payload, disjoint ranges, binary data and shrink/grow reuse.
    const size_t capacity = MAX_VALUE / 4 + 64;
    for (int i = 0; i < 4; ++i) store.put("blocks/" + std::to_string(i), std::string(capacity, '\x93'));
    for (size_t length : {MAX_VALUE / 4, size_t(1), size_t(63), size_t(65537), MAX_VALUE / 4}) {
        std::string input(length, '\xa7');
        std::vector<NativeStore::WriteChunk> chunks;
        for (int i = 0; i < 4; ++i)
            chunks.push_back({"blocks/" + std::to_string(i), capacity,
                {{3, std::string_view(input).substr(0, length / 2)},
                 {32 + length / 2, std::string_view(input).substr(length / 2)}}});
        FrameScratch::Scope scope(scratch);
        auto& delta = scope.frames();
        store.delta_many(chunks, delta);
        for (size_t i = 0; i < chunks.size(); ++i) {
            const auto old = store.get(chunks[i].key);
            std::string expected;
            for (const auto& r : chunks[i].ranges)
                for (size_t j = 0; j < r.bytes.size(); ++j) expected += old[r.offset+j] ^ r.bytes[j];
            assert(delta[i] == expected);
        }
        store.write_ranges_many(chunks);
        store.delta_many(chunks, delta);
        for (const auto& d : delta) assert(d == std::string(length, '\0'));
    }
    // Retention is bounded, even on exceptions; it never limits request size.
    FrameScratch small(1024);
    try {
        FrameScratch::Scope scope(small);
        scope.frames().resize(3);
        for (auto& f : scope.frames()) f.resize(MAX_VALUE);
        throw std::runtime_error("test unwind");
    } catch (const std::runtime_error&) {}
    {
        FrameScratch::Scope scope(small);
        size_t retained = 0;
        for (const auto& f : scope.frames()) { assert(f.empty()); retained += f.capacity(); }
        assert(retained <= 1024);
    }
    unsetenv("MOSAIC_BATCH_IO"); unsetenv("NATIVE_MEMORY_MB");
}
void parallel_ranges() {
    setenv("MOSAIC_BATCH_IO", "1", 1); setenv("MOSAIC_IO_THREADS", "4", 1);
    NativeStore store;
    Frames expected(4, std::string(4096, '\x25'));
    std::vector<NativeStore::ReadChunk> reads;
    std::vector<NativeStore::WriteChunk> writes;
    std::string a(127, '\x44'), b(191, '\x5a');
    for (size_t i = 0; i < expected.size(); ++i) {
        auto key = "blocks/" + std::to_string(i);
        store.put(key, expected[i]);
        reads.push_back({key, 4096, {{7, a.size()}, {2017, b.size()}}});
        writes.push_back({key, 4096, {{7, a}, {2017, b}}});
    }
    for (int round = 0; round < 100; ++round) {
        auto old = store.read_ranges_many(reads);
        auto deltas = store.delta_many(writes);
        for (size_t i = 0; i < expected.size(); ++i) {
            assert(old[i] == expected[i].substr(7, a.size()) + expected[i].substr(2017, b.size()));
            auto delta = old[i]; auto incoming = a + b;
            for (size_t j = 0; j < delta.size(); ++j) delta[j] ^= incoming[j];
            assert(deltas[i] == delta);
        }
        bool xor_delta = round % 2;
        store.write_ranges_many(writes, xor_delta);
        for (size_t i = 0; i < expected.size(); ++i)
            for (const auto& range : writes[i].ranges)
                for (size_t j = 0; j < range.bytes.size(); ++j) {
                    if (xor_delta) expected[i][range.offset+j] ^= range.bytes[j];
                    else expected[i][range.offset+j] = range.bytes[j];
                }
    }
    auto stats = store.stats();
    assert(stats.find("\"full_read_bytes\":0") != std::string::npos);
    assert(stats.find("\"full_write_bytes\":16384") != std::string::npos);
    assert(stats.find("\"threads\":4") != std::string::npos);
    auto bad = writes; bad.back().length = 4095;
    bool failed = false;
    try { store.write_ranges_many(bad); } catch (const Error&) { failed = true; }
    assert(failed);
    bad = writes; bad.back().key = bad.front().key; failed = false;
    try { store.write_ranges_many(bad); } catch (const Error&) { failed = true; }
    assert(failed);
    for (size_t i = 0; i < expected.size(); ++i) assert(store.get(writes[i].key) == expected[i]);
    // Single-chunk batches stay inline and don't dispatch any worker tasks.
    auto pool = store.stats(); auto tasks = pool.substr(pool.find("\"batch_io\""));
    writes.resize(1); reads.resize(1);
    store.delta_many(writes); store.write_ranges_many(writes); store.read_ranges_many(reads);
    pool = store.stats(); assert(tasks == pool.substr(pool.find("\"batch_io\"")));
    for (size_t i = 0; i < expected.size(); ++i) store.erase("blocks/" + std::to_string(i));
    assert(store.stats().find("\"groups\":{}") != std::string::npos);
    unsetenv("MOSAIC_BATCH_IO"); unsetenv("MOSAIC_IO_THREADS");
}
void retained_prefix() {
    setenv("NATIVE_MEMORY_MB", "1", 1);
    NativeStore store;
    std::string expected("abcdef\0ghij", 11);
    store.put("blocks/prefix", expected);
    size_t written = 0;
    for (const auto& prefix : Frames{"XY", "", std::string("12\0", 3), std::string(8192, 'A'), "Z"}) {
        const auto old = expected.size();
        expected = prefix + expected.substr(std::min(prefix.size(), old));
        store.write_prefix("blocks/prefix", old, prefix);
        written += prefix.size();
        assert(store.size("blocks/prefix") == expected.size());
        assert(store.get("blocks/prefix") == expected);
        assert(store.stats().find("\"range_write_bytes\":" + std::to_string(written)) != std::string::npos);
    }
    for (bool mismatch : {true, false}) {
        bool rejected = false;
        try { store.write_prefix("blocks/prefix", expected.size() + mismatch,
                                 mismatch ? "x" : std::string(1048577, 'x')); }
        catch (const Error&) { rejected = true; }
        assert(rejected && store.get("blocks/prefix") == expected);
    }
    unsetenv("NATIVE_MEMORY_MB");
}
int main() {
    {
        NativeStore partial;
        partial.repair_range("blocks/repair", 4096, 11, "abc");
        assert(partial.read_range("blocks/repair", 11, 3) == "abc");
        assert(!partial.range_available("blocks/repair", 4096, 0, 1));
        bool refused = false;
        try { partial.get("blocks/repair"); } catch (const Error& e) { refused = e.status == "NOT_FOUND"; }
        assert(refused);
        refused = false;
        try { partial.delta_many({{"blocks/repair", 4096, {{0, "x"}}}}); }
        catch (const Error& e) { refused = e.status == "NOT_FOUND"; }
        assert(refused);
        partial.write_range("blocks/repair", 11, "XYZ");
        partial.repair_range("blocks/repair", 4096, 0, std::string(11, 'a'));
        partial.repair_range("blocks/repair", 4096, 14, std::string(4082, 'z'));
        assert(partial.get("blocks/repair") == std::string(11, 'a') + "XYZ" + std::string(4082, 'z'));
        partial.erase("blocks/repair");
        partial.put("blocks/repair", "new");
        assert(partial.get("blocks/repair") == "new");
    }
    retained_prefix();
    reused_large_ranges();
    parallel_ranges();
    setenv("NATIVE_MEMORY_MB", "1", 1);
    NativeStore store;
    store.put("blocks/empty", ""); assert(store.contains("blocks/empty") && store.size("blocks/empty") == 0);
    store.put("blocks/a", std::string(4096, '\0'));
    std::string expected(4096, '\0');
    std::mt19937 rng(43);
    for (int i = 0; i < 1000; ++i) {
        size_t offset = rng() % 4096, length = rng() % (4097 - offset);
        std::string data(length, '\0'); for (auto& byte : data) byte = rng();
        assert(store.read_range("blocks/a", offset, length) == expected.substr(offset, length));
        store.write_range("blocks/a", offset, data, i % 2);
        for (size_t j = 0; j < length; ++j) {
            if (i % 2) expected[offset + j] ^= data[j]; else expected[offset + j] = data[j];
        }
    }
    auto stats = store.stats();
    assert(stats.find("\"full_read_bytes\":0") != std::string::npos);
    assert(stats.find("\"full_write_bytes\":4096") != std::string::npos);
    assert(store.get("blocks/a") == expected);
    bool rejected = false;
    try { store.write_range("blocks/a", 4095, "ab"); } catch (const Error&) { rejected = true; }
    assert(rejected && store.get("blocks/a") == expected);
    for (size_t next : {8192, 63, 0, 1, 4096}) {
        std::string delta(std::max(expected.size(), next), '\x7f');
        auto old_length = expected.size(); expected.resize(std::max(expected.size(), next), '\0');
        for (size_t i = 0; i < delta.size(); ++i) expected[i] ^= delta[i];
        expected.resize(next);
        store.resize_xor("blocks/a", old_length, next, delta);
        assert(store.get("blocks/a") == expected);
    }
    rejected = false;
    try { store.put("blocks/a", std::string(1048577, 'x')); } catch (const Error& e) { rejected = e.status == "UNAVAILABLE"; }
    assert(rejected && store.get("blocks/a") == expected);
    rejected = false;
    try { store.resize_xor("blocks/a", 4096, 1048577, ""); } catch (const Error& e) { rejected = e.status == "UNAVAILABLE"; }
    assert(rejected && store.get("blocks/a") == expected);
    store.erase("blocks/a"); store.erase("blocks/a"); store.erase("blocks/empty");
    assert(store.stats().find("\"groups\":{}") != std::string::npos);
    store.put("blocks/full", std::string(1048576, 'q'));
    rejected = false;
    try { store.put("buffers/extra", "x"); } catch (const Error& e) { rejected = e.status == "UNAVAILABLE"; }
    assert(rejected && !store.contains("buffers/extra"));
    assert(store.get("blocks/full") == std::string(1048576, 'q'));
    store.put("blocks/full", ""); store.put("buffers/extra", "x");
    assert(store.stats(true).find("\"buffers/extra\":1") != std::string::npos);
    std::cout << "PASS native ranges/random XOR/resize/zero/miss/bounds/capacity/stats\n";
}
