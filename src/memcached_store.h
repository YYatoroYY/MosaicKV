#pragma once
#include "serial_memcached_store.h"
#include "chunked_memcached_store.h"
#include "native_store.h"

namespace ec {
// Common storage interface for every scheme. Only the selected backend exists.
class AgentStore {
    std::unique_ptr<SerialMemcachedStore> serial_;
    std::unique_ptr<ChunkedMemcachedStore> parallel_;
    std::unique_ptr<NativeStore> native_;
    PartialRanges partial_; // memcached backend; NativeStore tracks its own ranges.
public:
    AgentStore() {
        if (storage_backend() == "native") native_ = std::make_unique<NativeStore>();
        else if (integer(env("MOSAIC_PARALLEL_IO", "0"), 0, 1)) parallel_ = std::make_unique<ChunkedMemcachedStore>();
        else serial_ = std::make_unique<SerialMemcachedStore>();
    }
    bool direct_ranges() const { return bool(native_); }
    NativeStore& native() { require(direct_ranges(), "native range I/O is disabled"); return *native_; }
    bool contains(const std::string& key) { return native_ ? native_->contains(key) : parallel_ ? parallel_->contains(key) : serial_->contains(key); }
    bool batch_enabled() const { return native_ || (serial_ && serial_->batch_enabled()); }
    std::vector<std::optional<std::string>> get_optional_many(const std::vector<std::string>& keys) {
        require(batch_enabled(), "Agent batch I/O is disabled");
        if (!native_ && !partial_.size()) return serial_->get_optional_many(keys);
        std::vector<std::optional<std::string>> values;
        values.reserve(keys.size());
        for (const auto& key : keys) {
            try { values.emplace_back(get(key)); }
            catch (const Error& error) { if (error.status != "NOT_FOUND") throw; values.emplace_back(std::nullopt); }
        }
        return values;
    }
    std::vector<std::string> get_many(const std::vector<std::string>& keys) {
        require(batch_enabled(), "Agent batch I/O is disabled");
        if (!native_ && !partial_.size()) return serial_->get_many(keys);
        std::vector<std::string> values;
        values.reserve(keys.size());
        for (const auto& key : keys) values.push_back(get(key));
        return values;
    }
    void put_many(const std::vector<std::string>& keys, const std::vector<std::string>& values) {
        require(batch_enabled() && keys.size() == values.size(), "invalid Agent batch write");
        if (!native_) { serial_->put_many(keys, values); return; }
        require(std::set<std::string>(keys.begin(), keys.end()).size() == keys.size(), "duplicate batch write key");
        for (size_t i = 0; i < keys.size(); ++i) put(keys[i], values[i]);
    }
    size_t size(const std::string& key) { return native_ ? native_->size(key) : parallel_ ? parallel_->size(key) : serial_->size(key); }
    std::string get(const std::string& key) {
        auto value = native_ ? native_->get(key) : parallel_ ? parallel_->get(key) : serial_->get(key);
        if (!native_) partial_.check(key, 0, value.size());
        return value;
    }
    void put(const std::string& key, const std::string& value) {
        if (native_) native_->put(key, value); else if (parallel_) parallel_->put(key, value); else serial_->put(key, value);
        partial_.erase(key);
    }
    void erase(const std::string& key) { if (native_) native_->erase(key); else if (parallel_) parallel_->erase(key); else serial_->erase(key); partial_.erase(key); }
    bool range_available(const std::string& key, size_t length, size_t offset, size_t count) {
        if (native_) return native_->range_available(key, length, offset, count);
        return contains(key) && size(key) == length && partial_.has(key, offset, count);
    }
    Frames read_ranges_many(const std::vector<NativeStore::ReadChunk>& chunks) {
        if (native_) return native_->read_ranges_many(chunks);
        Frames keys;
        for (const auto& chunk : chunks) {
            for (const auto& r : chunk.ranges) partial_.check(chunk.key, r.offset, r.length);
            keys.push_back(chunk.key);
        }
        // Memcached has no range GET: fetch each affected chunk once, in parallel,
        // then return only requested bytes over the inter-node connection.
        Frames full;
        if (serial_ && serial_->batch_enabled()) full = serial_->get_many(keys);
        else for (const auto& key : keys) full.push_back(parallel_ ? parallel_->get(key) : serial_->get(key));
        Frames result;
        for (size_t i = 0; i < chunks.size(); ++i) {
            require(full[i].size() == chunks[i].length, "range chunk length mismatch");
            result.emplace_back();
            for (const auto& r : chunks[i].ranges) result.back().append(full[i], r.offset, r.length);
        }
        return result;
    }
    void repair_range(const std::string& key, size_t length, size_t offset, std::string_view bytes) {
        if (native_) { native_->repair_range(key, length, offset, bytes); return; }
        require(offset <= length && bytes.size() <= length-offset, "invalid repair range");
        bool missing = !contains(key) || size(key) != length;
        auto value = missing ? std::string(length, '\0') : parallel_ ? parallel_->get(key) : serial_->get(key);
        require(value.size() == length, "repair chunk length mismatch");
        value.replace(offset, bytes.size(), bytes.data(), bytes.size());
        if (missing) { partial_.erase(key); partial_.begin(key); }
        if (parallel_) parallel_->put(key, value); else serial_->put(key, value);
        partial_.restored(key, offset, bytes.size(), length);
    }
    std::string layout(const std::string& key) { return native_ ? native_->layout(key) : parallel_ ? parallel_->layout(key) : serial_->layout(key); }
    std::string stats(bool inventory = false) { return native_ ? native_->stats(inventory) : parallel_ ? parallel_->stats(inventory) : serial_->stats(inventory); }
};
using MemcachedStore = AgentStore; // Compatibility for existing storage tests.
} // namespace ec
