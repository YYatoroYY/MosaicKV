#include "memcached_store.h"
#include <cassert>
#include <regex>
using namespace ec;

static std::vector<std::string> chunks(MemcachedStore& store, const std::string& key) {
    auto layout = store.layout(key);
    std::regex pattern("\"(chunks/[^\"]+)\"");
    std::vector<std::string> keys;
    for (auto it = std::sregex_iterator(layout.begin(), layout.end(), pattern); it != std::sregex_iterator(); ++it)
        keys.push_back((*it)[1]);
    return keys;
}

int main(int argc, char** argv) {
    try {
        const std::string mode = argc > 1 ? argv[1] : "suite";
        if (mode.compare(0, 4, "raw-") == 0) {
            MemcachedClient raw;
            for (int i = 2; i < argc; ++i) {
                if (mode == "raw-get") std::cout << raw.get(argv[i]);
                else if (mode == "raw-delete") raw.erase(argv[i]);
                else if (mode == "raw-exists") std::cout << raw.contains(argv[i]) << '\n';
                else throw std::runtime_error("unknown raw operation");
            }
            return 0;
        }
        MemcachedStore store;
        if (mode == "group-stats") {
            MemcachedClient raw;
            std::map<std::string, std::string> expected;
            auto verify = [&] {
                struct Usage { size_t count = 0, keys = 0, values = 0; };
                std::map<std::string, Usage> groups;
                for (const auto& [key, value] : expected) {
                    auto& g = groups[key.substr(0, key.find('/'))];
                    ++g.count; g.keys += key.size(); g.values += value.size();
                }
                std::ostringstream wanted;
                wanted << "\"groups\":{";
                bool first = true;
                for (const auto& [name, g] : groups) {
                    wanted << (first ? "" : ",") << '"' << name << "\":{\"entries\":" << g.count
                           << ",\"key_bytes\":" << g.keys << ",\"value_bytes\":" << g.values << '}';
                    first = false;
                }
                wanted << '}';
                assert(store.stats().find(wanted.str()) != std::string::npos);
                assert(store.stats(true).find(wanted.str()) != std::string::npos);
            };
            for (auto key : {"blocks/a", "blocks/b", "buffers/c", "deleted/d", "deleted-buffers/e"}) {
                expected[key] = key[0] == 'd' ? "" : "hello";
                store.put(key, expected[key]);
            }
            verify();
            expected["blocks/a"] = "short"; store.put("blocks/a", "short");
            expected["blocks/b"] = std::string(71, 'x'); raw.put("blocks/b", expected["blocks/b"]);
            assert(store.get("blocks/b") == expected["blocks/b"]); verify();
            raw.erase("buffers/c"); assert(!store.contains("buffers/c")); expected.erase("buffers/c"); verify();
            raw.erase("blocks/b");
            try { store.get("blocks/b"); assert(false); } catch (const Error& e) { assert(e.status == "NOT_FOUND"); }
            expected.erase("blocks/b"); verify();
            if (store.batch_enabled()) {
                store.put_many({"blocks/a", "blocks/b"}, {"", "BBBB"});
                expected["blocks/a"] = ""; expected["blocks/b"] = "BBBB"; verify();
                raw.erase("blocks/a"); raw.put("blocks/b", "refresh");
                auto got = store.get_optional_many({"blocks/a", "blocks/b"});
                assert(!got[0] && got[1] == "refresh");
                expected.erase("blocks/a"); expected["blocks/b"] = "refresh"; verify();
                store.put_many({"buffers/inline"}, {""}); expected["buffers/inline"] = ""; verify();
            }
            auto invalid = std::string(300, 'k');
            bool rejected = false;
            try { store.put(invalid, "invalid"); } catch (const Error&) { rejected = true; }
            assert(rejected); verify();
            for (const auto& [key, value] : expected) { store.erase(key); store.erase(key); }
            expected.clear(); verify();
            std::cout << "PASS group stats: real memcached writes/overwrites/read refresh/misses/batch/failed write/markers/delete\n";
            return 0;
        }
        if (mode == "batch-api") {
            assert(store.batch_enabled());
            store.put_many({"test/single"}, {std::string("a\0b", 3)});
            assert(store.get_many({"test/single"}) == Frames{std::string("a\0b", 3)});
            store.put_many({"test/single"}, {""});
            assert(store.get_many({"test/single"}) == Frames{""});
            store.erase("test/single");
            assert(!store.get_optional_many({"test/single"})[0]);
            bool missing = false;
            try { store.get_many({"test/single"}); } catch (const Error& e) { missing = e.status == "NOT_FOUND"; }
            assert(missing);
            store.put_many({"test/a", "test/b"}, {"A", "B"});
            assert(store.get_many({"test/a", "test/b"}) == Frames({"A", "B"}));
            bool duplicate = false;
            try { store.put_many({"test/a", "test/a"}, {"x", "y"}); } catch (const Error& e) { duplicate = e.status == "INVALID"; }
            assert(duplicate && store.get("test/a") == "A");
            auto counters = store.stats();
            assert(counters.find("\"batch_inline_reads\":4") != std::string::npos);
            assert(counters.find("\"batch_inline_writes\":2") != std::string::npos);
            std::cout << "PASS single-key batch: binary/empty values, optional miss, missing error, multi-key fallback and counters\n";
            return 0;
        }
        if (mode == "chunk-oom") {
            MemcachedClient raw;
            std::string old(65536, 'o');
            store.put("test/atomic", old);
            auto before = store.layout("test/atomic");
            raw.put("test/empty_slab", "");
            std::string filler(65536, 'f');
            size_t count = 0;
            for (; count < 1000; ++count) {
                try { raw.put("test/filler_" + std::to_string(count), filler); }
                catch (const Error&) { break; }
            }
            assert(count > 2 && count < 1000);
            // Free only two chunk-sized slots: an overwrite can write a prefix
            // but cannot finish its new generation in this 8 MiB cache.
            raw.erase("test/filler_0"); raw.erase("test/filler_1");
            bool failed = false;
            try { store.put("test/atomic", std::string(16 * 1024 * 1024, 'n')); }
            catch (const Error& e) { failed = e.status == "UNAVAILABLE"; }
            assert(failed && store.layout("test/atomic") == before && store.get("test/atomic") == old);
            auto stats = store.stats();
            assert(stats.find("\"pending_cleanup_generations\":0") != std::string::npos);
            assert(stats.find("\"aborted_chunk_fences\":256") != std::string::npos);
            std::cout << "PASS chunk OOM: partial new generation discarded/fenced; old value and manifest unchanged\n";
            return 0;
        }
        if (mode == "chunks") {
            MemcachedClient raw;
            std::string value(2 * 1024 * 1024 + 17, '\0');
            for (size_t i = 0; i < value.size(); ++i) value[i] = static_cast<char>(i % 256);
            store.put("test/chunks", value);
            auto keys = chunks(store, "test/chunks");
            assert(keys.size() > 4);
            std::string joined;
            for (const auto& key : keys) { auto part = raw.get(key); assert(part.size() <= 65536); joined += part; }
            assert(joined == value && store.get("test/chunks") == value);
            auto before = store.stats();
            for (int i = 0; i < 12; ++i) {
                value[0] = static_cast<char>(i);
                store.put("test/chunks", value);
                assert(store.get("test/chunks") == value);
            }
            for (const auto& key : keys) assert(!raw.contains(key));
            keys = chunks(store, "test/chunks");
            raw.erase(keys[1]);
            bool missing = false;
            try { store.get("test/chunks"); } catch (const Error& e) { missing = e.status == "UNAVAILABLE"; }
            assert(missing);
            store.put("test/chunks", value);
            store.erase("test/chunks"); store.erase("test/chunks");
            for (const auto& key : keys) assert(!raw.contains(key));
            auto after = store.stats();
            std::regex tids("\"tid\":[0-9]+");
            std::vector<std::string> a, b;
            for (auto it = std::sregex_iterator(before.begin(), before.end(), tids); it != std::sregex_iterator(); ++it) a.push_back(it->str());
            for (auto it = std::sregex_iterator(after.begin(), after.end(), tids); it != std::sregex_iterator(); ++it) b.push_back(it->str());
            assert(a.size() == 4 && a == b && after.find("\"max_active\":4") != std::string::npos);
            std::cout << "PASS chunks: independent physical read, parallel assembly, COW overwrite/delete, missing chunk rejected, 4 persistent workers\n";
            return 0;
        }
        if (mode == "get" && argc == 3) { std::cout << store.get(argv[2]); return 0; }
        if (mode == "delete" && argc == 3) { store.erase(argv[2]); return 0; }
        if (mode == "stats") { std::cout << store.stats(); return 0; }
        if (mode == "oom") {
            std::string value(256 * 1024, 'x');
            size_t stored = 0;
            bool failed = false;
            for (; stored < 256; ++stored) {
                try { store.put("test/pressure_" + std::to_string(stored), value); }
                catch (const Error& e) { assert(e.status == "UNAVAILABLE"); failed = true; break; }
            }
            assert(failed && stored > 0);
            // -M must preserve previously stored items instead of evicting them.
            for (size_t i = 0; i < stored; ++i)
                assert(store.get("test/pressure_" + std::to_string(i)) == value);
            std::cout << "PASS memcached OOM returns error; all " << stored << " prior items retained\n";
            return 0;
        }
        assert(mode == "suite");
        store.put("test/empty", "");
        assert(store.contains("test/empty") && store.get("test/empty").empty());
        std::string binary(2 * 1024 * 1024 + 17, '\0');
        for (size_t i = 0; i < binary.size(); ++i) binary[i] = static_cast<char>(i % 256);
        store.put("test/binary", binary);
        assert(store.get("test/binary") == binary);
        store.put("test/binary", "short\r\n");
        assert(store.get("test/binary") == "short\r\n");
        // RMW records can contain request, absolute block and delta (3*MAX_BLOCK).
        std::string record(3 * MAX_BLOCK + 4096, 'r');
        store.put("test/large_record", record);
        assert(store.get("test/large_record") == record);
        for (auto key : {"test/empty", "test/binary", "test/large_record"}) {
            store.erase(key); store.erase(key); assert(!store.contains(key));
        }
        try { store.get("test/missing"); assert(false); }
        catch (const Error& e) { assert(e.status == "NOT_FOUND"); }
        std::cout << "PASS memcached binary/empty/>1MiB/large RMW record/replace/idempotent delete/miss\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
