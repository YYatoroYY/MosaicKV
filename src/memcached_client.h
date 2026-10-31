#pragma once
#include "common.h"
#include <libmemcached/memcached.h>
#include <chrono>
#include <map>
#include <memory>
#include <thread>

namespace ec {
// One instance per owning thread; never shared between I/O workers.
class MemcachedClient {
    std::unique_ptr<memcached_st, decltype(&memcached_free)> client_{memcached_create(nullptr), memcached_free};
    void check(memcached_return_t rc, const char* operation) {
        if (rc == MEMCACHED_SUCCESS) return;
        std::string message = std::string("memcached ") + operation + ": " + memcached_strerror(client_.get(), rc);
        // A failed write may have taken effect. Never replay it implicitly.
        memcached_quit(client_.get());
        throw Error("UNAVAILABLE", message);
    }
public:
    MemcachedClient() {
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
            if (rc == MEMCACHED_SUCCESS) return;
            memcached_quit(client_.get());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        check(rc, "startup");
    }
    MemcachedClient(const MemcachedClient&) = delete;
    MemcachedClient& operator=(const MemcachedClient&) = delete;
    bool contains(const std::string& key) {
        auto rc = memcached_exist(client_.get(), key.data(), key.size());
        if (rc == MEMCACHED_NOTFOUND) { return false; }
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
            throw Error("NOT_FOUND", "memcached entry missing: " + key);
        }
        check(rc, "get");
        return length ? std::string(data.get(), length) : std::string();
    }
    void put(const std::string& key, const std::string& value) {
        // TTL=0; acknowledged set, without buffering or noreply.
        check(memcached_set(client_.get(), key.data(), key.size(), value.data(), value.size(), 0, 0), "set");
    }
    void erase(const std::string& key) {
        auto rc = memcached_delete(client_.get(), key.data(), key.size(), 0);
        if (rc != MEMCACHED_NOTFOUND) check(rc, "delete");
    }
    memcached_stat_st stats() {
        memcached_return_t rc;
        auto release = [this](memcached_stat_st* p) { if (p) memcached_stat_free(client_.get(), p); };
        std::unique_ptr<memcached_stat_st, decltype(release)> server(memcached_stat(client_.get(), nullptr, &rc), release);
        check(rc, "stats");
        if (!server) throw std::bad_alloc();
        return *server;
    }
    void add(const std::string& key, const std::string& value) {
        check(memcached_add(client_.get(), key.data(), key.size(), value.data(), value.size(), 0, 0), "add");
    }
};
} // namespace ec
