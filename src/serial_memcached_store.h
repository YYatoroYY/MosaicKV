#pragma once
#include "common.h"
#include "memcached_io.h"
#include "length_index.h"
#include <optional>
#include <libmemcached/memcached.h>
#include <chrono>
#include <map>
#include <memory>
#include <thread>

namespace ec {
// Solely owned by the node REP thread. The index contains lengths, never values.
// Every object, parity, replica, transaction and retirement fence lives in memcached.
class SerialMemcachedStore {
    std::unique_ptr<memcached_st, decltype(&memcached_free)> client_{memcached_create(nullptr), memcached_free};
    LengthIndex lengths_;
    std::unique_ptr<MemcachedIo> batch_io_;
    uint64_t batch_inline_reads_ = 0, batch_inline_writes_ = 0;
    void check(memcached_return_t rc, const char* operation) {
        if (rc == MEMCACHED_SUCCESS) return;
        std::string message = std::string("memcached ") + operation + ": " + memcached_strerror(client_.get(), rc);
        // A failed write may have taken effect. Never replay it implicitly.
        memcached_quit(client_.get());
        throw Error("UNAVAILABLE", message);
    }
public:
    SerialMemcachedStore() {
        if (!client_) throw std::bad_alloc();
        check(memcached_server_add(client_.get(), "127.0.0.1",
                                   integer(env("MEMCACHED_PORT", "11211"), 1, 65535)), "server_add");
        const auto timeout = integer(env("MEMCACHED_TIMEOUT_MS", "1000"), 10, 60000);
        for (auto setting : {std::make_pair(MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, uint64_t(1)),
                             {MEMCACHED_BEHAVIOR_TCP_NODELAY, 1},
                             {MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT, uint64_t(timeout)},
                             {MEMCACHED_BEHAVIOR_POLL_TIMEOUT, uint64_t(timeout)},
                             {MEMCACHED_BEHAVIOR_SND_TIMEOUT, uint64_t(timeout) * 1000},
                             {MEMCACHED_BEHAVIOR_RCV_TIMEOUT, uint64_t(timeout) * 1000}})
            check(memcached_behavior_set(client_.get(), setting.first, setting.second), "behavior");
        // The local supervisor starts both children together. Only startup probes retry.
        memcached_return_t rc = MEMCACHED_FAILURE;
        for (int attempt = 0; attempt < 50; ++attempt) {
            rc = memcached_version(client_.get());
            if (rc == MEMCACHED_SUCCESS) {
                if (integer(env("MOSAIC_BATCH_IO", "0"), 0, 1))
                    batch_io_ = std::make_unique<MemcachedIo>(integer(env("MOSAIC_IO_THREADS", "4"), 1, 32));
                return;
            }
            memcached_quit(client_.get());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        check(rc, "startup");
    }
    SerialMemcachedStore(const SerialMemcachedStore&) = delete;
    bool batch_enabled() const { return static_cast<bool>(batch_io_); }
    std::vector<std::optional<std::string>> get_optional_many(const std::vector<std::string>& keys) {
        require(batch_enabled(), "Agent batch I/O is disabled");
        if (keys.size() == 1) {
            ++batch_inline_reads_;
            std::vector<std::optional<std::string>> result(1);
            try { result[0] = get(keys[0]); }
            catch (const Error& error) { if (error.status != "NOT_FOUND") throw; }
            return result;
        }
        CacheBatch batch(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            batch.add(batch_io_->submit(i, MemcachedIo::Op::Get, keys[i]));
        std::vector<std::optional<std::string>> result(keys.size());
        size_t i = 0;
        for (auto& task : batch) {
            try { result[i] = task.get(); lengths_.record(keys[i], result[i]->size()); }
            catch (const Error& error) {
                if (error.status != "NOT_FOUND") throw;
                lengths_.erase(keys[i]);
            }
            ++i;
        }
        return result;
    }
    std::vector<std::string> get_many(const std::vector<std::string>& keys) {
        require(batch_enabled(), "Agent batch I/O is disabled");
        if (keys.size() == 1) {
            // The REP thread owns this connection. Multiple-key batches still
            // use independent worker connections and drain before returning.
            ++batch_inline_reads_;
            std::vector<std::string> result;
            result.push_back(get(keys[0]));
            return result;
        }
        auto values = get_optional_many(keys);
        std::vector<std::string> result;
        result.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            if (!values[i]) throw Error("NOT_FOUND", "memcached entry missing: " + keys[i]);
            result.push_back(std::move(*values[i]));
        }
        return result;
    }
    void put_many(const std::vector<std::string>& keys, const std::vector<std::string>& values) {
        require(batch_enabled() && keys.size() == values.size(), "invalid Agent batch write");
        if (keys.size() == 1) {
            ++batch_inline_writes_;
            put(keys[0], values[0]);
            return;
        }
        require(std::set<std::string>(keys.begin(), keys.end()).size() == keys.size(), "duplicate batch write key");
        CacheBatch batch(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            batch.add(batch_io_->submit(i, MemcachedIo::Op::Put, keys[i], values[i], batch.token()));
        size_t i = 0;
        std::exception_ptr failure;
        for (auto& task : batch) {
            try { task.get(); lengths_.record(keys[i], values[i].size()); }
            catch (...) { if (!failure) failure = std::current_exception(); }
            ++i;
        }
        if (failure) std::rethrow_exception(failure);
    }
    SerialMemcachedStore& operator=(const SerialMemcachedStore&) = delete;
    bool contains(const std::string& key) {
        auto rc = memcached_exist(client_.get(), key.data(), key.size());
        if (rc == MEMCACHED_NOTFOUND) { lengths_.erase(key); return false; }
        check(rc, "exists");
        return true;
    }
    std::string get(const std::string& key) {
        size_t length = 0;
        uint32_t flags = 0;
        memcached_return_t rc;
        std::unique_ptr<char, decltype(&std::free)> data(
            memcached_get(client_.get(), key.data(), key.size(), &length, &flags, &rc), std::free);
        if (rc == MEMCACHED_NOTFOUND) {
            lengths_.erase(key);
            throw Error("NOT_FOUND", "memcached entry missing: " + key);
        }
        check(rc, "get");
        lengths_.record(key, length);
        return length ? std::string(data.get(), length) : std::string();
    }
    void put(const std::string& key, const std::string& value) {
        // TTL=0; acknowledged set, without buffering or noreply.
        check(memcached_set(client_.get(), key.data(), key.size(), value.data(), value.size(), 0, 0), "set");
        lengths_.record(key, value.size());
    }
    size_t size(const std::string& key) {
        auto length = lengths_.size(key);
        return length ? *length : get(key).size();
    }
    void erase(const std::string& key) {
        auto rc = memcached_delete(client_.get(), key.data(), key.size(), 0);
        if (rc != MEMCACHED_NOTFOUND) check(rc, "delete");
        lengths_.erase(key);
    }
    std::string layout(const std::string& key) {
        auto value = get(key);
        require(key.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_/-") == std::string::npos, "invalid layout key");
        return "{\"bytes\":" + std::to_string(value.size()) + ",\"chunks\":[\"" + key + "\"]}";
    }
    std::string stats(bool inventory = false) {
        memcached_return_t rc;
        auto release = [this](memcached_stat_st* p) { if (p) memcached_stat_free(client_.get(), p); };
        std::unique_ptr<memcached_stat_st, decltype(release)> server(memcached_stat(client_.get(), nullptr, &rc), release);
        check(rc, "stats");
        if (!server) throw std::bad_alloc();
        auto agent_rss = process_rss_bytes(), cache_rss = process_rss_bytes(server->pid);
        std::ostringstream out;
        out << "{\"storage_mode\":\"memory\",\"storage_backend\":\"memcached\",\"storage_layout\":\"single_item\",\"parallel_io\":false,\"rss_bytes\":" << agent_rss + cache_rss
            << ",\"agent_rss_bytes\":" << agent_rss << ",\"memcached_rss_bytes\":" << cache_rss
            << ",\"memcached\":{\"pid\":" << server->pid << ",\"bytes\":" << server->bytes
            << ",\"threads\":" << server->threads << ",\"limit_maxbytes\":" << server->limit_maxbytes << ",\"curr_items\":" << server->curr_items
            << ",\"evictions\":" << server->evictions << "},\"groups\":{";
        bool first = true;
        for (const auto& [name, group] : lengths_.groups()) {
            out << (first ? "" : ",") << '"' << name << "\":{\"entries\":" << group.entries
                << ",\"key_bytes\":" << group.key_bytes << ",\"value_bytes\":" << group.value_bytes << '}';
            first = false;
        }
        out << '}';
        if (inventory) {
            out << ",\"entries\":{";
            first = true;
            for (const auto& [key, length] : lengths_.entries()) {
                if (key.compare(0, 7, "blocks/") && key.compare(0, 8, "buffers/")) continue;
                out << (first ? "" : ",") << '"' << key << "\":" << length;
                first = false;
            }
            out << '}';
        }
        out << ",\"batch_inline_reads\":" << batch_inline_reads_
            << ",\"batch_inline_writes\":" << batch_inline_writes_;
        out << ",\"batch_io\":" << (batch_io_ ? batch_io_->stats() : "null") << '}';
        return out.str();
    }
};
} // namespace ec
