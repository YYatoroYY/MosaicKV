#pragma once
#include "memcached_io.h"
#include <map>
#include <numeric>

namespace ec {
// The node REP thread owns logical manifests; workers own all chunk I/O.
// No value cache: manifests contain only generation IDs, lengths and chunk counts.
class ChunkedMemcachedStore {
    struct Generation { std::string id; size_t bytes, parts; };
    struct Retired {
        std::string logical;
        Generation generation;
        bool fence;
        std::vector<size_t> pending;
    };
    MemcachedClient admin_;
    MemcachedIo io_{static_cast<size_t>(integer(env("MOSAIC_IO_THREADS", "4"), 1, 32))};
    size_t chunk_bytes_ = integer(env("MOSAIC_CHUNK_BYTES", "262144"), 4096, MAX_BLOCK);
    std::map<std::string, Generation> entries_;
    std::map<std::string, Retired> retired_;
    uint64_t fenced_chunks_ = 0, cleanup_failures_ = 0;
    size_t single_lane_ = 0;
    static std::string chunk_key(const Generation& g, size_t part) {
        return "chunks/" + g.id + "_" + std::to_string(part);
    }
    static size_t offset(const Generation& g, size_t part) { return g.bytes * part / g.parts; }
    const Generation& lookup(const std::string& key) const {
        auto it = entries_.find(key);
        if (it == entries_.end()) throw Error("NOT_FOUND", "logical memcached entry missing: " + key);
        return it->second;
    }
    void retire(const std::string& key, const Generation& g, bool fence) {
        Retired r{key, g, fence, std::vector<size_t>(g.parts)};
        std::iota(r.pending.begin(), r.pending.end(), size_t(0));
        retired_.emplace(g.id, std::move(r));
    }
    void cleanup() {
        size_t visited = 0;
        for (auto it = retired_.begin(); it != retired_.end() && visited < 8; ++visited) {
            auto& r = it->second;
            CacheBatch batch(r.pending.size());
            for (size_t part : r.pending)
                batch.add(io_.submit(part, r.fence ? MemcachedIo::Op::Fence : MemcachedIo::Op::Delete,
                                     chunk_key(r.generation, part), {}, batch.token()));
            size_t index = 0, remaining = 0;
            for (auto& future : batch) {
                size_t part = r.pending[index++];
                try { future.get(); if (r.fence) ++fenced_chunks_; }
                catch (...) { r.pending[remaining++] = part; ++cleanup_failures_; }
            }
            r.pending.resize(remaining);
            if (r.pending.empty()) it = retired_.erase(it);
            else ++it;
        }
    }
public:
    ChunkedMemcachedStore() = default;
    ChunkedMemcachedStore(const ChunkedMemcachedStore&) = delete;
    ChunkedMemcachedStore& operator=(const ChunkedMemcachedStore&) = delete;
    bool contains(const std::string& key) const { return entries_.count(key); }
    size_t size(const std::string& key) const { return lookup(key).bytes; }
    std::string get(const std::string& key) {
        auto g = lookup(key);
        CacheBatch batch(g.parts);
        auto lane = g.parts == 1 ? single_lane_++ : 0;
        for (size_t i = 0; i < g.parts; ++i)
            batch.add(io_.submit(lane + i, MemcachedIo::Op::Get, chunk_key(g, i), {}, batch.token()));
        std::string result; result.reserve(g.bytes);
        size_t i = 0;
        for (auto& future : batch) {
            std::string part;
            try { part = future.get(); }
            catch (const Error& e) {
                if (e.status == "NOT_FOUND") throw Error("UNAVAILABLE", "memcached chunk missing: " + key);
                throw;
            }
            if (part.size() != offset(g, i + 1) - offset(g, i))
                throw Error("UNAVAILABLE", "memcached chunk length mismatch: " + key);
            result += part; ++i;
        }
        return result;
    }
    void put(const std::string& key, const std::string& value) {
        cleanup();
        size_t count = value.size() < 64 ? 1 : std::max(io_.size(), (value.size() + chunk_bytes_ - 1) / chunk_bytes_);
        Generation next{uuid(), value.size(), count};
        auto old = entries_.find(key);
        // Copy the old descriptor before submitting any writes.
        std::unique_ptr<Generation> previous;
        if (old != entries_.end()) previous = std::make_unique<Generation>(old->second);
        try {
            CacheBatch batch(count);
            auto lane = count == 1 ? single_lane_++ : 0;
            for (size_t i = 0; i < count; ++i) {
                auto begin = offset(next, i), length = offset(next, i + 1) - begin;
                batch.add(io_.submit(lane + i, MemcachedIo::Op::Add, chunk_key(next, i), value.substr(begin, length), batch.token()));
            }
            for (auto& f : batch) f.get();
            // Single REP owner: publish only after every ADD acknowledgment.
            entries_.insert_or_assign(key, next);
        } catch (...) {
            auto error = std::current_exception();
            // Batch has drained. Empty permanent items fence late ADDs after a
            // lost reply; they are never deleted/reused during this node lifetime.
            retire(key, next, true);
            cleanup();
            std::rethrow_exception(error);
        }
        if (previous) retire(key, *previous, false);
        cleanup(); // Failed reclamation stays queued; the new generation is valid.
    }
    void erase(const std::string& key) {
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            retire(key, it->second, false);
            entries_.erase(it);
        }
        cleanup();
        for (const auto& [id, r] : retired_)
            if (r.logical == key) throw Error("UNAVAILABLE", "memcached chunk deletion pending: " + key);
    }
    std::string layout(const std::string& key) const {
        const auto& g = lookup(key);
        std::ostringstream out;
        out << "{\"generation\":\"" << g.id << "\",\"bytes\":" << g.bytes << ",\"chunks\":[";
        for (size_t i = 0; i < g.parts; ++i) out << (i ? "," : "") << '"' << chunk_key(g, i) << '"';
        out << "]}"; return out.str();
    }
    std::string stats(bool inventory = false) {
        cleanup();
        auto server = admin_.stats();
        struct Usage { size_t entries = 0, key_bytes = 0, value_bytes = 0, chunks = 0; };
        std::map<std::string, Usage> groups;
        size_t physical_chunks = 0, pending_chunks = 0;
        for (const auto& [key, g] : entries_) {
            auto& group = groups[key.substr(0, key.find('/'))];
            ++group.entries; group.key_bytes += key.size(); group.value_bytes += g.bytes;
            group.chunks += g.parts; physical_chunks += g.parts;
        }
        for (const auto& [id, r] : retired_) pending_chunks += r.pending.size();
        auto agent_rss = process_rss_bytes(), cache_rss = process_rss_bytes(server.pid);
        std::ostringstream out;
        out << "{\"storage_mode\":\"memory\",\"storage_backend\":\"memcached\",\"storage_layout\":\"chunked\",\"parallel_io\":true"
            << ",\"rss_bytes\":" << agent_rss + cache_rss << ",\"agent_rss_bytes\":" << agent_rss
            << ",\"memcached_rss_bytes\":" << cache_rss << ",\"chunk_bytes\":" << chunk_bytes_
            << ",\"active_chunks\":" << physical_chunks << ",\"pending_cleanup_generations\":" << retired_.size()
            << ",\"pending_cleanup_chunks\":" << pending_chunks << ",\"aborted_chunk_fences\":" << fenced_chunks_
            << ",\"chunk_cleanup_failures\":" << cleanup_failures_ << ",\"chunk_io\":" << io_.stats()
            << ",\"memcached\":{\"pid\":" << server.pid << ",\"threads\":" << server.threads
            << ",\"bytes\":" << server.bytes << ",\"limit_maxbytes\":" << server.limit_maxbytes
            << ",\"curr_items\":" << server.curr_items << ",\"evictions\":" << server.evictions << "},\"groups\":{";
        bool first = true;
        for (const auto& [name, group] : groups) {
            out << (first ? "" : ",") << '"' << name << "\":{\"entries\":" << group.entries
                << ",\"key_bytes\":" << group.key_bytes << ",\"value_bytes\":" << group.value_bytes
                << ",\"chunks\":" << group.chunks << '}'; first = false;
        }
        out << '}';
        if (inventory) {
            out << ",\"entries\":{"; first = true;
            for (const auto& [key, g] : entries_) {
                if (key.compare(0, 7, "blocks/") && key.compare(0, 8, "buffers/")) continue;
                out << (first ? "" : ",") << '"' << key << "\":" << g.bytes; first = false;
            }
            out << '}';
        }
        out << '}'; return out.str();
    }
};
} // namespace ec
