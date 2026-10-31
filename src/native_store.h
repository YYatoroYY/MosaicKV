#pragma once
#include "common.h"
#include "native_io.h"
#include "partial_ranges.h"
#include "atomic_xor.h"
#include <map>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace ec {
// The node service mutex protects the directory, sizes and counters. Range batches pin
// existing entry buffers until every worker completes, including failure paths.
// Workers touch distinct chunk buffers only; no directory mutations or resizing.
class NativeStore {
public:
    struct ReadRange { size_t offset, length; };
    struct WriteRange { size_t offset; std::string_view bytes; };
    struct ReadChunk { std::string key; size_t length; std::vector<ReadRange> ranges; };
    struct WriteChunk { std::string key; size_t length; std::vector<WriteRange> ranges; };
private:
    struct Usage { size_t entries = 0, key_bytes = 0, value_bytes = 0; };
    std::unordered_map<std::string, std::string> entries_;
    PartialRanges partial_;
    std::map<std::string, Usage> groups_;
    size_t bytes_ = 0;
    size_t limit_ = size_t(integer(env("NATIVE_MEMORY_MB", "512"), 1, 1048576)) * 1024 * 1024;
    uint64_t full_read_ = 0, full_write_ = 0, range_read_ = 0, range_write_ = 0;
    uint64_t inline_reads_ = 0, inline_writes_ = 0;
    // Destroy/join workers before the directory and entry buffers.
    std::unique_ptr<NativeIo> io_;
    void run(size_t count, NativeIo::Op op, const std::function<void(size_t)>& action) {
        if (io_ && count > 1) { io_->run(count, op, action); return; }
        if (op != NativeIo::Op::Write) inline_reads_ += count;
        if (op == NativeIo::Op::Write || op == NativeIo::Op::Xor) inline_writes_ += count;
        for (size_t i = 0; i < count; ++i) action(i);
    }
    std::vector<std::string*> prepare(const std::vector<WriteChunk>& chunks, size_t& total) {
        std::vector<std::string*> values; values.reserve(chunks.size());
        std::set<std::string> unique;
        total = 0;
        for (const auto& chunk : chunks) {
            require(unique.insert(chunk.key).second, "duplicate native write chunk");
            auto& value = entry(chunk.key);
            require(value.size() == chunk.length, "native chunk length mismatch");
            size_t end = 0;
            for (const auto& range : chunk.ranges) {
                bounds(value.size(), range.offset, range.bytes.size());
                partial_.check(chunk.key, range.offset, range.bytes.size());
                require(range.offset >= end, "overlapping native write ranges");
                end = range.offset + range.bytes.size(); total += range.bytes.size();
            }
            values.push_back(&value);
        }
        return values;
    }
    static std::string group(const std::string& key) { return key.substr(0, key.find('/')); }
    std::string& entry(const std::string& key) {
        auto it = entries_.find(key);
        if (it == entries_.end()) throw Error("NOT_FOUND", "native entry missing: " + key);
        return it->second;
    }
    void capacity(size_t old, size_t next) const {
        if (next > limit_ || bytes_ - old > limit_ - next)
            throw Error("UNAVAILABLE", "native payload capacity exceeded; eviction disabled");
    }
    static void bounds(size_t size, size_t offset, size_t length) {
        require(offset <= size && length <= size - offset, "native range outside entry");
    }
public:
    NativeStore() {
        if (integer(env("MOSAIC_BATCH_IO", "0"), 0, 1))
            io_ = std::make_unique<NativeIo>(integer(env("MOSAIC_IO_THREADS", "4"), 1, 32));
    }
    Frames read_ranges_many(const std::vector<ReadChunk>& chunks) {
        std::vector<const std::string*> values; values.reserve(chunks.size());
        Frames result; result.reserve(chunks.size());
        std::set<std::string> unique;
        size_t total = 0;
        for (const auto& chunk : chunks) {
            require(unique.insert(chunk.key).second, "duplicate native read chunk");
            const auto& value = entry(chunk.key);
            require(value.size() == chunk.length, "native chunk length mismatch");
            size_t count = 0;
            for (const auto& range : chunk.ranges) {
                bounds(value.size(), range.offset, range.length);
                partial_.check(chunk.key, range.offset, range.length); count += range.length;
            }
            values.push_back(&value); result.emplace_back(count, '\0'); total += count;
        }
        run(chunks.size(), NativeIo::Op::Read, [&](size_t i) {
            size_t cursor = 0;
            for (const auto& range : chunks[i].ranges) {
                if (range.length) std::memcpy(result[i].data() + cursor, values[i]->data() + range.offset, range.length);
                cursor += range.length;
            }
        });
        range_read_ += total;
        return result;
    }
    // Caller-owned output permits bounded request scratch reuse. It must not
    // alias incoming ranges; workers finish before the output can be reused.
    void delta_many(const std::vector<WriteChunk>& chunks, Frames& result) {
        size_t total = 0; auto values = prepare(chunks, total);
        result.resize(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            const auto& chunk = chunks[i];
            size_t count = 0; for (const auto& range : chunk.ranges) count += range.bytes.size();
            result[i].resize(count);
        }
        run(chunks.size(), NativeIo::Op::Delta, [&](size_t i) {
            size_t cursor = 0;
            for (const auto& range : chunks[i].ranges) {
                char* dst = result[i].data() + cursor;
                const char* old = values[i]->data() + range.offset;
                const char* src = range.bytes.data();
                for (size_t j = 0; j < range.bytes.size(); ++j)
                    dst[j] = old[j] ^ src[j];
                cursor += range.bytes.size();
            }
        });
        range_read_ += total;
    }
    Frames delta_many(const std::vector<WriteChunk>& chunks) {
        Frames result;
        delta_many(chunks, result);
        return result;
    }
    void write_ranges_many(const std::vector<WriteChunk>& chunks, bool xor_delta = false) {
        size_t total = 0; auto values = prepare(chunks, total);
        run(chunks.size(), xor_delta ? NativeIo::Op::Xor : NativeIo::Op::Write, [&](size_t i) {
            for (const auto& range : chunks[i].ranges) {
                if (xor_delta) {
                    char* dst = values[i]->data() + range.offset;
                    atomic_xor(dst, range.bytes);
                } else if (!range.bytes.empty())
                    std::memcpy(values[i]->data() + range.offset, range.bytes.data(), range.bytes.size());
            }
        });
        if (xor_delta) range_read_ += total;
        range_write_ += total;
    }
    bool contains(const std::string& key) const { return entries_.count(key); }
    size_t size(const std::string& key) { return entry(key).size(); }
    std::string get(const std::string& key) {
        const auto& value = entry(key);
        partial_.check(key, 0, value.size());
        auto result = value;
        full_read_ += value.size();
        return result;
    }
    void put(const std::string& key, const std::string& value, bool complete = true) {
        auto it = entries_.find(key);
        size_t old = it == entries_.end() ? 0 : it->second.size();
        capacity(old, value.size());
        // Allocate before replacing any published bytes or counters.
        auto copy = value;
        auto [g, inserted] = groups_.try_emplace(group(key));
        if (it == entries_.end()) {
            try { entries_.emplace(key, std::move(copy)); }
            catch (...) { if (inserted) groups_.erase(g); throw; }
            ++g->second.entries; g->second.key_bytes += key.size();
        } else it->second.swap(copy);
        g->second.value_bytes = g->second.value_bytes - old + value.size();
        bytes_ = bytes_ - old + value.size(); full_write_ += value.size();
        if (complete) partial_.erase(key);
    }
    void erase(const std::string& key) {
        partial_.erase(key);
        auto it = entries_.find(key);
        if (it == entries_.end()) return;
        auto g = groups_.find(group(key));
        --g->second.entries; g->second.key_bytes -= key.size();
        g->second.value_bytes -= it->second.size(); bytes_ -= it->second.size();
        entries_.erase(it);
        if (!g->second.entries) groups_.erase(g);
    }
    std::string read_range(const std::string& key, size_t offset, size_t length) {
        const auto& value = entry(key); bounds(value.size(), offset, length);
        partial_.check(key, offset, length);
        auto result = value.substr(offset, length);
        range_read_ += length;
        return result;
    }
    void write_range(const std::string& key, size_t offset, std::string_view bytes, bool xor_delta = false) {
        auto& value = entry(key); bounds(value.size(), offset, bytes.size());
        partial_.check(key, offset, bytes.size());
        if (xor_delta) {
            char* dst = value.data() + offset;
            atomic_xor(dst, bytes);
            range_read_ += bytes.size();
        } else if (!bytes.empty()) std::memcpy(value.data() + offset, bytes.data(), bytes.size());
        range_write_ += bytes.size();
    }
    bool range_available(const std::string& key, size_t length, size_t offset, size_t count) {
        return contains(key) && size(key) == length && partial_.has(key, offset, count);
    }
    void repair_range(const std::string& key, size_t length, size_t offset, std::string_view bytes) {
        bounds(length, offset, bytes.size());
        if (!contains(key) || size(key) != length) {
            // Allocate the backing chunk locally; only restored ranges are valid.
            partial_.erase(key);
            partial_.begin(key);
            try { put(key, std::string(length, '\0'), false); }
            catch (...) { partial_.erase(key); throw; }
        }
        auto& value = entry(key);
        require(value.size() == length, "repair chunk length mismatch");
        std::memcpy(value.data() + offset, bytes.data(), bytes.size());
        range_write_ += bytes.size();
        partial_.restored(key, offset, bytes.size(), length);
    }
    // InPlace keeps old suffix bytes after logical shrink. Grow only when the
    // new prefix exceeds the physical size; touch no retained suffix bytes.
    void write_prefix(const std::string& key, size_t old_length, std::string_view prefix) {
        auto& value = entry(key);
        require(value.size() == old_length, "native prefix length mismatch");
        partial_.check(key, 0, value.size());
        const auto next = std::max(old_length, prefix.size());
        capacity(old_length, next);
        auto& usage = groups_.at(group(key));
        value.resize(next, '\0'); // Allocation, if needed, precedes mutation.
        if (!prefix.empty()) std::memcpy(value.data(), prefix.data(), prefix.size());
        usage.value_bytes = usage.value_bytes - old_length + next;
        bytes_ = bytes_ - old_length + next;
        range_write_ += prefix.size();
    }
    void resize_xor(const std::string& key, size_t old_length, size_t new_length, std::string_view delta) {
        auto& value = entry(key);
        require(value.size() == old_length && delta.size() <= std::max(old_length, new_length), "native parity length mismatch");
        partial_.check(key, 0, value.size());
        capacity(old_length, new_length);
        auto& usage = groups_.at(group(key));
        value.resize(std::max(old_length, new_length), '\0'); // May allocate, before mutation.
        char* dst = value.data();
        const char* src = delta.data();
        for (size_t i = 0; i < delta.size(); ++i) dst[i] ^= src[i];
        value.resize(new_length);
        usage.value_bytes = usage.value_bytes - old_length + new_length;
        bytes_ = bytes_ - old_length + new_length;
        range_read_ += std::min(delta.size(), old_length); range_write_ += delta.size();
    }
    std::string layout(const std::string& key) {
        require(key.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_/-") == std::string::npos, "invalid layout key");
        return "{\"bytes\":" + std::to_string(size(key)) + ",\"chunks\":[\"" + key + "\"]}";
    }
    std::string stats(bool inventory = false) const {
        const auto rss = process_rss_bytes();
        std::ostringstream out;
        out << "{\"storage_mode\":\"memory\",\"storage_backend\":\"native\",\"storage_layout\":\"direct_ranges\","
            << "\"parallel_io\":" << (io_ ? "true" : "false") << ",\"range_io\":true,\"rss_bytes\":" << rss << ",\"agent_rss_bytes\":" << rss
            << ",\"memcached_rss_bytes\":0,\"memcached\":null,\"native\":{\"bytes\":" << bytes_
            << ",\"limit_payload_bytes\":" << limit_ << ",\"partial_repair_chunks\":" << partial_.size()
            << ",\"curr_items\":" << entries_.size() << ",\"evictions\":0,"
            << "\"full_read_bytes\":" << full_read_ << ",\"full_write_bytes\":" << full_write_
            << ",\"range_read_bytes\":" << range_read_ << ",\"range_write_bytes\":" << range_write_
            << "},\"groups\":{";
        bool first = true;
        for (const auto& [name, usage] : groups_) {
            out << (first ? "" : ",") << '"' << name << "\":{\"entries\":" << usage.entries
                << ",\"key_bytes\":" << usage.key_bytes << ",\"value_bytes\":" << usage.value_bytes << '}';
            first = false;
        }
        out << '}';
        if (inventory) {
            out << ",\"entries\":{"; first = true;
            for (const auto& [key, value] : entries_) {
                if (key.compare(0, 7, "blocks/") && key.compare(0, 8, "buffers/")) continue;
                out << (first ? "" : ",") << '"' << key << "\":" << value.size(); first = false;
            }
            out << '}';
        }
        out << ",\"batch_inline_reads\":" << inline_reads_ << ",\"batch_inline_writes\":" << inline_writes_
            << ",\"batch_io\":" << (io_ ? io_->stats() : "null") << '}';
        return out.str();
    }
};
} // namespace ec
