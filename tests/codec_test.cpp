#include "codec.h"
#include <functional>
#include <random>
using namespace ec;
int main() {
    try {
        std::mt19937 random(20261002);
        // Reuse outputs across maximum-size, tiny, odd and growing deltas.
        // Independent scalar GF multiplication catches stale/padded bytes.
        {
            Codec codec(6, 3, MAX_VALUE);
            Frames outputs;
            for (size_t length : {MAX_VALUE, size_t(1), size_t(63), size_t(64), size_t(65537), MAX_VALUE}) {
                std::string delta(length, '\0');
                for (char& c : delta) c = static_cast<char>(random());
                const auto original = delta;
                for (int index : {0, 5}) {
                    codec.parity_delta_into(index, delta, outputs);
                    if (delta != original) throw std::runtime_error("modified delta source");
                    for (int j = 0; j < 3; ++j) {
                        unsigned char coefficient = gf_inv((6 + j) ^ index);
                        unsigned char table[256];
                        for (int b = 0; b < 256; ++b) table[b] = gf_mul(coefficient, b);
                        if (outputs[j].size() != length) throw std::runtime_error("reused delta length mismatch");
                        for (size_t b = 0; b < length; ++b)
                            if (static_cast<unsigned char>(outputs[j][b]) != table[static_cast<unsigned char>(delta[b])])
                                throw std::runtime_error("reused parity/GF mismatch");
                    }
                }
            }
        }
        size_t cases = 0;
        for (auto params : std::vector<std::pair<int, int>>{{2, 1}, {4, 2}, {6, 3}}) {
            auto [k, m] = params;
            Codec codec(k, m, 128);
            std::string data(k * 128, '\0');
            for (char& c : data) c = static_cast<char>(random());
            const auto expected = codec.encode(data);
            // Differential oracle: ordinary RMW must match a complete stripe re-encode.
            for (int index = 0; index < k; ++index) {
                for (int length : {1, 17, 63, 64, 127, 128}) {
                    int offset = 128 - length;
                    auto updated = data;
                    std::string delta(length, '\0');
                    for (int j = 0; j < length; ++j) {
                        auto pos = index * 128 + offset + j;
                        updated[pos] = static_cast<char>(random());
                        delta[j] = data[pos] ^ updated[pos];
                    }
                    auto changes = codec.parity_delta(index, delta);
                    auto encoded = codec.encode(updated);
                    for (int j = 0; j < m; ++j) {
                        auto parity = expected[k + j];
                        for (int b = 0; b < length; ++b) parity[offset + b] ^= changes[j][b];
                        if (parity != encoded[k + j]) throw std::runtime_error("RMW/full-encode mismatch");
                    }
                }
            }
            for (int failures = 0; failures <= m; ++failures) {
                auto damaged = expected;
                std::function<void(int, int)> visit = [&](int start, int left) {
                    if (!left) {
                        if (codec.recover(damaged) != expected) throw std::runtime_error("EC recovery mismatch");
                        for (int target = 0; target < k + m; ++target) if (damaged[target].empty()) {
                            for (size_t length : {size_t(1), size_t(17), size_t(64), size_t(128)}) {
                                std::vector<std::string> ranges(k + m);
                                for (int i = 0; i < k + m; ++i)
                                    if (!damaged[i].empty()) ranges[i] = damaged[i].substr(128-length, length);
                                if (codec.recover_target(ranges, target, length) != expected[target].substr(128-length, length))
                                    throw std::runtime_error("fragment target recovery mismatch");
                            }
                        }
                        ++cases;
                        return;
                    }
                    for (int i = start; i <= k + m - left; ++i) {
                        damaged[i].clear();
                        visit(i + 1, left - 1);
                        damaged[i] = expected[i];
                    }
                };
                visit(0, failures);
            }
            auto damaged = expected;
            for (int i = 0; i <= m; ++i) damaged[i].clear();
            bool rejected = false;
            try { codec.recover(damaged); } catch (const Error& e) { rejected = e.status == "UNAVAILABLE"; }
            if (!rejected) throw std::runtime_error("must reject insufficient shards");
            for (size_t length : std::vector<size_t>{0, 1, 127, 129, static_cast<size_t>(k * 128 - 1)}) {
                auto blocks = codec.encode(data.substr(0, length));
                blocks[0].clear();
                auto restored = codec.recover(blocks);
                std::string joined;
                for (int i = 0; i < k; ++i) joined += restored[i];
                if (joined.substr(0, length) != data.substr(0, length)) throw std::runtime_error("padding mismatch");
            }
        }
        std::cout << "PASS: " << cases << " erasure combinations; RMW/full-encode equivalence; padding and insufficient-shard checks\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
