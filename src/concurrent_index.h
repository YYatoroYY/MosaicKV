#pragma once
#include "metadata_memory.h"
#include <array>
#include <atomic>
#include <memory>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace ec {
template<class T> struct CountedAllocator {
    using value_type = T;
    std::atomic<uint64_t>* bytes;
    explicit CountedAllocator(std::atomic<uint64_t>* b) : bytes(b) {}
    template<class U> CountedAllocator(const CountedAllocator<U>& a) : bytes(a.bytes) {}
    T* allocate(size_t n) { auto p = std::allocator<T>{}.allocate(n); *bytes += metadata_memory::allocation(p); return p; }
    void deallocate(T* p, size_t n) { *bytes -= metadata_memory::allocation(p); std::allocator<T>{}.deallocate(p, n); }
    template<class U> bool operator==(const CountedAllocator<U>& b) const { return bytes == b.bytes; }
    template<class U> bool operator!=(const CountedAllocator<U>& b) const { return !(*this == b); }
};

// Hash buckets synchronize lookup/creation, while each entry independently
// serializes writers. Readers atomically pin an immutable version and keep a
// shared per-key lock only against in-place writes, never relocations/deletes.
template<class Version, class Pending> class ConcurrentIndex {
public:
    struct Entry {
        std::mutex writer;
        std::shared_mutex readers;
        std::shared_ptr<const Version> value;
        std::unique_ptr<Pending> pending; // Transaction storage exists only while a write is unsettled.
        std::atomic<bool> unsettled{false}, inplace{false};
        std::shared_ptr<const Version> load() const { return std::atomic_load(&value); }
        void publish(std::shared_ptr<const Version> next) { std::atomic_store(&value, std::move(next)); }
    };
private:
    std::atomic<uint64_t> bytes_{0}; // Outlives all counted containers/records.
    using Pair = std::pair<const std::string, std::shared_ptr<Entry>>;
    using Map = std::unordered_map<std::string, std::shared_ptr<Entry>, std::hash<std::string>, std::equal_to<std::string>, CountedAllocator<Pair>>;
    struct Bucket {
        mutable std::shared_mutex mutex;
        Map entries;
        explicit Bucket(std::atomic<uint64_t>* b) : entries(0, std::hash<std::string>{}, std::equal_to<std::string>{}, CountedAllocator<Pair>(b)) {}
    };
    std::array<std::unique_ptr<Bucket>, 64> buckets_;
    Bucket& bucket(const std::string& key) const { return *buckets_[std::hash<std::string>{}(key) % buckets_.size()]; }
public:
    ConcurrentIndex() { for (auto& b : buckets_) b = std::make_unique<Bucket>(&bytes_); }
    std::shared_ptr<Entry> find(const std::string& key, bool create = false) {
        auto& b = bucket(key);
        { std::shared_lock<std::shared_mutex> l(b.mutex); auto it = b.entries.find(key); if (it != b.entries.end()) return it->second; }
        if (!create) return {};
        std::unique_lock<std::shared_mutex> l(b.mutex);
        auto [it, added] = b.entries.try_emplace(key);
        if (added) {
            try { it->second = std::allocate_shared<Entry>(CountedAllocator<Entry>(&bytes_)); }
            catch (...) { b.entries.erase(it); throw; }
        }
        return it->second;
    }
    template<class... Args> std::shared_ptr<const Version> version(Args&&... args) {
        return std::allocate_shared<Version>(CountedAllocator<Version>(&bytes_), std::forward<Args>(args)...);
    }
    template<class T, class... Args> std::shared_ptr<T> record(Args&&... args) {
        return std::allocate_shared<T>(CountedAllocator<T>(&bytes_), std::forward<Args>(args)...);
    }
    auto snapshot() const {
        std::vector<std::pair<std::string, std::shared_ptr<const Version>>> result;
        for (const auto& b : buckets_) {
            std::shared_lock<std::shared_mutex> l(b->mutex);
            for (const auto& [key, entry] : b->entries) if (auto v = entry->load()) result.emplace_back(key, std::move(v));
        }
        return result;
    }
    void sweep() {
        for (auto& b : buckets_) {
            std::unique_lock<std::shared_mutex> l(b->mutex);
            for (auto it = b->entries.begin(); it != b->entries.end();)
                if (it->second.use_count() == 1 && !it->second->unsettled && !it->second->load()) it = b->entries.erase(it);
                else ++it;
        }
    }
    void prune(const std::string& key, const std::shared_ptr<Entry>& expected) {
        auto& b = bucket(key); std::unique_lock<std::shared_mutex> l(b.mutex);
        auto it = b.entries.find(key);
        if (it != b.entries.end() && it->second == expected && expected.use_count() == 2 &&
            !expected->unsettled && !expected->load()) b.entries.erase(it);
    }
    uint64_t metadata_bytes() const {
        uint64_t result = bytes_.load();
        for (const auto& b : buckets_) {
            std::shared_lock<std::shared_mutex> l(b->mutex);
            result += metadata_memory::allocation(b.get());
            for (const auto& [key, e] : b->entries) { (void)e; result += metadata_memory::string(key); }
        }
        return result;
    }
};
} // namespace ec
