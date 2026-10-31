#include "length_index.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
using namespace ec;

static void verify(const LengthIndex& index, const std::map<std::string, size_t>& expected) {
    assert(index.entries() == expected);
    std::map<std::string, LengthIndex::Usage> scanned;
    for (const auto& [key, length] : expected) {
        auto& g = scanned[key.substr(0, key.find('/'))];
        ++g.entries; g.key_bytes += key.size(); g.value_bytes += length;
    }
    assert(scanned.size() == index.groups().size());
    for (const auto& [name, g] : scanned) {
        const auto& actual = index.groups().at(name);
        assert(g.entries == actual.entries && g.key_bytes == actual.key_bytes && g.value_bytes == actual.value_bytes);
    }
}

int main(int argc, char** argv) {
    LengthIndex index;
    std::map<std::string, size_t> expected;
    std::mt19937 rng(42);
    for (int i = 0; i < 10000; ++i) {
        auto key = std::string(i % 3 ? "blocks/" : "deleted/") + std::to_string(rng() % 300);
        if (rng() % 4 == 0) { index.erase(key); expected.erase(key); }
        else { size_t n = rng() % 4097; index.record(key, n); expected[key] = n; index.record(key, n); }
        if (i % 100 == 0) verify(index, expected);
    }
    verify(index, expected);
    for (const auto& [key, length] : expected) { assert(index.size(key) == length); index.erase(key); index.erase(key); }
    assert(index.groups().empty() && index.entries().empty() && !index.size("missing"));
    index.record("deleted-buffers/empty", 0);
    assert(index.size("deleted-buffers/empty") == 0);
    index.erase("deleted-buffers/empty");
    std::cout << "PASS incremental groups: randomized overwrite/read-refresh/delete/zero-length/idempotence\n";
    if (argc == 2) {
        size_t count = std::stoull(argv[1]);
        for (size_t i = 0; i < count; ++i) {
            const char* prefix = i % 8 == 0 ? "blocks/" : (i % 2 ? "deleted/" : "deleted-buffers/");
            index.record(prefix + std::string(32, 'a') + std::to_string(i), i % 8 ? 0 : 4096);
        }
        using Clock = std::chrono::steady_clock;
        auto start = Clock::now();
        std::map<std::string, LengthIndex::Usage> scanned;
        for (const auto& [key, length] : index.entries()) {
            auto& g = scanned[key.substr(0, key.find('/'))];
            ++g.entries; g.key_bytes += key.size(); g.value_bytes += length;
        }
        double scan_ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
        volatile size_t sink = 0;
        start = Clock::now();
        for (int n = 0; n < 10000; ++n)
            for (const auto& [name, g] : index.groups()) sink += g.entries + g.key_bytes + g.value_bytes;
        double summary_us = std::chrono::duration<double, std::micro>(Clock::now() - start).count() / 10000;
        size_t total = 0;
        for (const auto& [name, g] : scanned) {
            const auto& actual = index.groups().at(name);
            assert(g.entries == actual.entries && g.key_bytes == actual.key_bytes && g.value_bytes == actual.value_bytes);
            total += g.entries;
        }
        assert(total == count);
        std::cout << "{\"entries\":" << count << ",\"full_scan_ms\":" << scan_ms
                  << ",\"cached_summary_us\":" << summary_us << "}\n";
    }
}
