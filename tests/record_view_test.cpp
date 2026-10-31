#include "record_view.h"
#include <cassert>
#include <random>
using namespace ec;
int main() {
    std::mt19937 rng(7319);
    for (int iteration = 0; iteration < 3000; ++iteration) {
        Frames owned;
        for (unsigned n = rng() % 16; n; --n) {
            std::string field(rng() % 513, '\0');
            for (auto& byte : field) byte = static_cast<char>(rng());
            owned.push_back(std::move(field));
        }
        const auto raw = pack_record(owned);
        auto views = unpack_record_views(raw);
        assert(views.size() == owned.size());
        for (size_t i = 0; i < views.size(); ++i) {
            assert(views[i] == owned[i]);
            assert(views[i].data() >= raw.data() && views[i].data() + views[i].size() <= raw.data() + raw.size());
        }
        assert(pack_record_views(owned) == raw && pack_record_views(views) == raw);
        std::string nested = pack_record_views({"header", raw, std::string_view("\0x\xff", 3)});
        auto outer = unpack_record_views(nested);
        assert(pack_record_views(unpack_record_views(outer[1])) == raw);
        assert(outer[2] == std::string_view("\0x\xff", 3));
        // Every truncated prefix must match the owning parser's validation.
        for (size_t cut = 0; cut < std::min<size_t>(nested.size(), 30); ++cut) {
            std::string prefix = nested.substr(0, cut);
            bool a = false, b = false;
            try { unpack_record(prefix); } catch (...) { a = true; }
            try { unpack_record_views(prefix); } catch (...) { b = true; }
            assert(a == b);
        }
    }
    assert(pack_record_views({}).empty());
    bool rejected = false;
    try { unpack_record_views(std::string_view("\xff\xff\xff\xffx", 5)); } catch (...) { rejected = true; }
    assert(rejected);
    std::cout << "PASS record views: 3000 binary/nested wire-equivalence cases, borrowed ranges, malformed/truncated lengths\n";
}
