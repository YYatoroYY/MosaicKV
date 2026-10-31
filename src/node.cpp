#include "codec.h"
#include "parity_workers.h"
#include "memcached_store.h"
#include "profile.h"
#include "record_view.h"
#include "frame_scratch.h"
#include "node_holes.h"
#include "transaction_journal.h"
#include "concurrent_server.h"
using namespace ec;

class Node {
    std::mutex service_mutex_;
    std::condition_variable transactions_changed_;
    inline static thread_local std::unique_lock<std::mutex>* service_lock_ = nullptr;
    struct NetworkWait {
        NetworkWait() { service_lock_->unlock(); }
        ~NetworkWait() { service_lock_->lock(); }
    };
    struct NodeReplies : PendingReplies {
        using PendingReplies::PendingReplies;
        ~NodeReplies() {
            bool outstanding = false;
            for (auto& reply : *this) outstanding |= reply.valid();
            if (!outstanding) return;
            NetworkWait waiting;
            for (auto& reply : *this) if (reply.valid()) reply.wait();
        }
    };
    std::string id_;
    int k_, m_, bytes_;
    AgentStore memory_;
    std::map<std::pair<int, int>, std::unique_ptr<Codec>> codecs_;
    Context context_;
    std::vector<std::string> nodes_;
    ParityWorkers writers_;
    mosaic::NodeHoles holes_{static_cast<size_t>(k_)};
    // Grow on demand up to legal request sizes: 16 MiB delta plus M encoded
    // deltas (64 MiB retained payload capacity for 6+3). This is a retention
    // budget, not a request size limit or eager reservation. Async RPCs own
    // separate serialized buffers; key/codec counts cannot multiply this cache.
    FrameScratch batch_deltas_{MAX_VALUE}, batch_parity_{size_t(m_) * MAX_VALUE};
    struct Traffic { uint64_t requests = 0, request_bytes = 0, reply_bytes = 0, errors = 0; };
    std::map<std::string, Traffic> traffic_; // Protected by service_mutex_.
    uint64_t traffic_started_ns_ = Profile::now();
    std::string traffic_stats() const {
        uint64_t requests = 0, received = 0, sent = 0, errors = 0;
        std::ostringstream operations;
        bool first = true;
        for (const auto& [op, count] : traffic_) {
            requests += count.requests; received += count.request_bytes; sent += count.reply_bytes; errors += count.errors;
            operations << (first ? "" : ",") << '\"' << op << "\":{\"requests\":" << count.requests
                << ",\"request_bytes\":" << count.request_bytes << ",\"reply_bytes\":" << count.reply_bytes
                << ",\"errors\":" << count.errors << '}';
            first = false;
        }
        auto peer_sent = writers_.sent_bytes(), peer_received = writers_.received_bytes();
        std::ostringstream out;
        out << "{\"unit\":\"rpc_application_frame_bytes\",\"enabled\":" << (traffic_enabled() ? "true" : "false")
            << ",\"workload_phase\":\"" << workload_phase() << "\",\"elapsed_ns\":" << (resize_phase() ? Profile::now() - traffic_started_ns_ : 0)
            << ",\"received_bytes\":" << received + peer_received << ",\"sent_bytes\":" << sent + peer_sent
            << ",\"served_request_bytes\":" << received << ",\"served_reply_bytes\":" << sent
            << ",\"peer_request_bytes\":" << peer_sent << ",\"peer_reply_bytes\":" << peer_received
            << ",\"served_requests\":" << requests << ",\"errors\":" << errors
            << ",\"operations\":{" << operations.str() << "}}";
        return out.str();
    }
    static void valid_name(const std::string& name) {
        require(!name.empty() && name.size() <= 80 &&
                name.find_first_not_of("0123456789abcdef_") == std::string::npos, "invalid block/transaction name");
    }
    std::string checked_block(const std::string& name) {
        valid_name(name);
        auto path = "blocks/" + name;
        if (!memory_.contains(path)) throw Error("UNAVAILABLE", "block missing: " + name);
        auto block = memory_.get(path);
        if (block.size() > MAX_BLOCK)
            throw Error("UNAVAILABLE", "invalid block length: " + name);
        return block;
    }
    Frames patch(const Frames& request) {
        // PATCH node tx block "offset" deltaParity
        require(request.size() == 6, "PATCH frame count");
        valid_name(request[2]);
        valid_name(request[3]);
        std::istringstream header(request[4]);
        int offset = -1;
        std::string extra;
        header >> offset;
        require(header && !(header >> extra) && offset >= 0 && static_cast<size_t>(offset) < MAX_BLOCK && !request[5].empty() &&
                request[5].size() <= MAX_BLOCK - offset, "invalid parity patch");
        if (memory_.direct_ranges()) {
            memory_.native().write_range("blocks/" + request[3], offset, request[5], true);
            return {"OK", ""};
        }
        auto block = checked_block(request[3]);
        require(static_cast<size_t>(offset) < block.size() && request[5].size() <= block.size() - offset, "parity patch outside block");
        for (size_t j = 0; j < request[5].size(); ++j) block[offset + j] ^= request[5][j];
        memory_.put("blocks/" + request[3], block);
        return {"OK", ""};
    }
    Frames patch_resize(const Frames& request) {
        // Full-object XOR delta, with implicit zero extension and final truncation.
        require(request.size() == 6, "PATCH_RESIZE frame count");
        valid_name(request[2]); valid_name(request[3]);
        std::istringstream header(request[4]);
        size_t old_length = 0, new_length = 0; std::string extra;
        header >> old_length >> new_length;
        require(header && !(header >> extra) && old_length <= MAX_BLOCK && new_length <= MAX_BLOCK &&
                request[5].size() <= std::max(old_length, new_length), "invalid resize parity patch");
        if (memory_.direct_ranges()) {
            memory_.native().resize_xor("blocks/" + request[3], old_length, new_length, request[5]);
            return {"OK", ""};
        }
        auto block = checked_block(request[3]);
        require(block.size() == old_length, "resize parity length mismatch");
        // Apply the complete old/new object delta before changing the final
        // parity length. Newly extended parity bytes start at zero locally.
        block.resize(std::max(old_length, new_length), '\0');
        for (size_t i = 0; i < request[5].size(); ++i) block[i] ^= request[5][i];
        block.resize(new_length);
        memory_.put("blocks/" + request[3], block);
        return {"OK", ""};
    }
    Frames rmw_resize(const Frames& request) {
        // RMW_RESIZE node tx data_block "index K M old_data old_parity new_parity; routes" value
        require(request.size() == 6, "RMW_RESIZE frame count");
        valid_name(request[2]); valid_name(request[3]);
        std::istringstream header(request[4]);
        int index = -1, k = 0, m = 0;
        size_t old_length = 0, old_parity = 0, new_parity = 0;
        header >> index >> k >> m >> old_length >> old_parity >> new_parity;
        require(header && k >= 1 && k <= k_ && m == m_ && index >= 0 && index < k &&
                old_length <= old_parity && old_parity <= MAX_BLOCK && new_parity <= MAX_BLOCK &&
                request[5].size() <= new_parity, "invalid resize RMW header");
        std::vector<int> targets; std::vector<std::string> names; std::set<int> unique;
        for (int j = 0; j < m_; ++j) {
            int node = -1; std::string name; header >> node >> name;
            require(header && node >= 0 && node < static_cast<int>(nodes_.size()) &&
                    std::to_string(node) != id_ && unique.insert(node).second, "invalid resize parity route");
            valid_name(name); targets.push_back(node); names.push_back(name);
        }
        std::string extra; require(!(header >> extra), "trailing resize parity routes");
        // old_length is physical data length, which may exceed the current
        // logical object length after a shrink. Keep its tail in the codeword.
        require(new_parity >= old_parity, "retained-tail RMW cannot truncate parity");
        const auto key = "blocks/" + request[3];
        const size_t span = request[5].size();
        std::string block, old;
        if (memory_.direct_ranges()) {
            require(memory_.size(key) == old_length, "resize data length mismatch");
            old = memory_.native().read_range(key, 0, std::min(old_length, span));
        } else {
            block = checked_block(request[3]);
            require(block.size() == old_length, "resize data length mismatch");
        }
        auto delta = request[5];
        const auto& previous = memory_.direct_ranges() ? old : block;
        for (size_t i = 0; i < std::min(old_length, span); ++i) delta[i] ^= previous[i];
        if (!memory_.direct_ranges()) {
            // Overwrite the prefix without erasing the retained suffix. Bytes
            // past the physical end are virtual zeros only during encoding.
            block.resize(std::max(old_length, span), '\0');
            if (span) std::memcpy(block.data(), request[5].data(), span);
        }
        std::vector<std::string> deltas(m_);
        if (span) {
            // RS coefficients depend on K/M, not the object's current length.
            // One max-span codec per K avoids a cache entry for every resize length.
            auto& codec = codecs_[{k, static_cast<int>(MAX_BLOCK)}];
            if (!codec) codec = std::make_unique<Codec>(k, m_, MAX_BLOCK);
            deltas = codec->parity_delta(index, delta);
        }
        PendingReplies pending(m_);
        for (int j = 0; j < m_; ++j)
            pending.add(writers_.submit(j, nodes_[targets[j]],
                {"PATCH_RESIZE", std::to_string(targets[j]), request[2] + "_" + std::to_string(j), names[j],
                 std::to_string(old_parity) + " " + std::to_string(new_parity), std::move(deltas[j])}, nullptr, targets[j]));
        // Local data write overlaps the persistent parity workers. All results
        // are drained; a partial write/lost ACK is reported and never replayed.
        std::string error;
        try {
            if (memory_.direct_ranges()) memory_.native().write_prefix(key, old_length, request[5]);
            else memory_.put(key, block);
        }
        catch (const std::exception& e) { error = e.what(); }
        for (auto& task : pending) try { task.get(); }
            catch (const std::exception& e) { error = e.what(); }
        if (!error.empty()) throw Error("UNAVAILABLE", "resize RMW failed; replay is disabled: " + error);
        return {"OK", ""};
    }
    Frames rmw(const Frames& request) {
        // RMW node tx data_block "index offset K M B; parity routes..." new slice
        require(request.size() == 6, "RMW frame count");
        valid_name(request[2]);
        valid_name(request[3]);
        std::istringstream header(request[4]);
        int index = -1, offset = -1, k = 0, m = 0, bytes = 0;
        header >> index >> offset >> k >> m >> bytes;
        require(header && index >= 0 && index < k && offset >= 0 && offset < bytes &&
                k >= 1 && k <= k_ && m == m_ && bytes >= 64 && bytes <= static_cast<int>(MAX_BLOCK) && !request[5].empty() &&
                request[5].size() <= static_cast<size_t>(bytes - offset), "invalid RMW header/configuration");
        std::vector<int> targets;
        std::vector<std::string> names;
        std::set<int> unique;
        for (int j = 0; j < m_; ++j) {
            int target = -1;
            std::string name;
            header >> target >> name;
            require(header && target >= 0 && target < static_cast<int>(nodes_.size()) &&
                    std::to_string(target) != id_ && unique.insert(target).second, "invalid parity route");
            valid_name(name);
            targets.push_back(target); names.push_back(name);
        }
        std::string extra;
        require(!(header >> extra), "trailing parity routes");
        const auto key = "blocks/" + request[3];
        std::string block;
        auto delta = request[5];
        if (memory_.direct_ranges()) {
            require(memory_.size(key) <= static_cast<size_t>(bytes), "RMW block length mismatch");
            auto old = memory_.native().read_range(key, offset, delta.size());
            for (size_t j = 0; j < delta.size(); ++j) delta[j] ^= old[j];
        } else {
            block = checked_block(request[3]);
            require(block.size() <= static_cast<size_t>(bytes) &&
                    static_cast<size_t>(offset) <= block.size() && request[5].size() <= block.size() - offset, "RMW block length mismatch");
            for (size_t j = 0; j < delta.size(); ++j) delta[j] ^= block[offset + j];
            block.replace(offset, request[5].size(), request[5]);
        }
        auto& codec = codecs_[{k, bytes}];
        if (!codec) codec = std::make_unique<Codec>(k, m_, bytes);
        auto deltas = codec->parity_delta(index, delta);
        PendingReplies pending(m_);
        for (int j = 0; j < m_; ++j) {
            Frames patch_request{"PATCH", std::to_string(targets[j]), request[2] + "_" + std::to_string(j), names[j],
                                 std::to_string(offset), std::move(deltas[j])};
            pending.add(writers_.submit(j, nodes_[targets[j]], std::move(patch_request), nullptr, targets[j]));
        }
        std::string error;
        for (auto& task : pending) {
            try { task.get(); }
            catch (const std::exception& e) { error = e.what(); }
        }
        if (!error.empty()) throw Error("UNAVAILABLE", "RMW failed; replay is disabled: " + error);
        if (memory_.direct_ranges()) memory_.native().write_range(key, offset, request[5]);
        else memory_.put(key, block);
        return {"OK", ""};
    }
#include "node_batch.inc"
#include "node_transactions.inc"
public:
    Node() : id_(env("NODE_ID", "0")), k_(integer(env("EC_K", "6"), 1, 31)),
        m_(integer(env("EC_M", "3"), 1, 31)), bytes_(integer(env("BLOCK_SIZE", "4096"), 64, 1024 * 1024)),
        nodes_(load_nodes(env("NODES_FILE", "/app/nodes.conf"))),
        writers_(context_, m_, integer(env("NODE_TIMEOUT_MS", "1000"), 10, 60000)) {
        require(k_ + m_ <= 32 && nodes_.size() >= static_cast<size_t>(k_ + m_), "need valid K+M nodes");
        integer(id_, 0, nodes_.size() - 1);
        std::cout << "node=" << id_ << " parity_workers=" << m_ << " storage=memory backend=" << storage_backend() << std::endl;
    }
    Frames handle(const Frames& request) {
        std::unique_lock<std::mutex> lock(service_mutex_);
        service_lock_ = &lock;
        if (!traffic_enabled() || !traffic_operation(request)) return handle_request(request);
        // Count received requests, including application failures and retries.
        // Match serve()'s reply construction without changing protocol behavior.
        const auto& op = request[0] == "MOSAIC" && request.size() == 5 ? request[2] : request[0];
        auto known = op == "WRITE" || op == "ZERO_CHUNK" || op == "HOLES" || op == "TX_RMW" || op == "TX_PATCH" ||
            op == "TX_COMMIT" || op == "TX_ABORT" || op == "TX_PARITY_COMMIT" || op == "TX_PARITY_ABORT" ||
            op == "READ" || op == "DELETE" || op == "CLONE" || op == "CHECK" ||
            op == "RMW" || op == "PATCH" || op == "RMW_RESIZE" || op == "PATCH_RESIZE" || op == "READ_BATCH" || op == "CHECK_BATCH" || op == "RMW_BATCH" ||
            op == "READ_RANGES" || op == "CHECK_RANGES" || op == "REPAIR_RANGES" || op == "PATCH_BATCH" || op == "BUFFER_WRITE" || op == "BUFFER_READ" || op == "BUFFER_DELETE" ||
            op == "BUFFER_DELETE_BATCH";
        auto& count = traffic_[known ? op : "OTHER"];
        ++count.requests; count.request_bytes += frame_bytes(request);
        try {
            auto reply = handle_request(request);
            count.reply_bytes += frame_bytes(reply);
            if (reply[0] == "MOSAIC") { if (unpack_record(reply[1])[0] != "OK") ++count.errors; }
            else if (reply[0] != "OK") ++count.errors;
            return reply;
        } catch (const Error& error) {
            ++count.errors; count.reply_bytes += error.status.size() + std::char_traits<char>::length(error.what());
            throw;
        } catch (const std::exception& error) {
            ++count.errors; count.reply_bytes += 8 + std::char_traits<char>::length(error.what());
            throw;
        }
    }
    Frames handle_request(const Frames& request) {
        require(request.size() >= 2 && request[1] == id_, "wrong node identity or missing frames");
        const auto& op = request[0];
        if (op == "MOSAIC") {
            require(request.size() == 5, "MOSAIC envelope frame count");
            auto inner = unpack_record(request[4]);
            require(inner.size() >= 2 && inner[0] == request[2] && inner[0] != "MOSAIC", "invalid Mosaic envelope");
            const auto fmax = integer(request[3], 1, 4096);
            Frames reply;
            try { reply = handle_request(inner); }
            catch (const Error& e) { reply = {e.status, e.what()}; }
            catch (const std::exception& e) { reply = {"INTERNAL", e.what()}; }
            reply.push_back(id_); reply.push_back(holes_.report(fmax));
            return {"MOSAIC", pack_record(reply)};
        }
        if (op == "HOLES") return holes_.handle(request);
        if (op == "ZERO_CHUNK") {
            require(request.size() == 4, "ZERO_CHUNK frame count");
            valid_name(request[2]);
            const auto length = static_cast<size_t>(integer(request[3], 64, MAX_BLOCK));
            require(!memory_.contains("deleted/" + request[2]), "EC shard has been retired");
            const auto key = "blocks/" + request[2];
            if (memory_.contains(key)) require(memory_.size(key) == length, "chunk size mismatch");
            else memory_.put(key, std::string(length, '\0'));
            return {"OK", ""};
        }
        if (op == "TX_RMW") return batch_rmw(request);
        if (op == "TX_PATCH") return transaction_patch(request);
        if (op == "TX_PARITY_ABORT" || op == "TX_PARITY_COMMIT") return transaction_parity_finish(request, op == "TX_PARITY_ABORT");
        if (op == "TX_ABORT" || op == "TX_COMMIT") return transaction_data_finish(request, op == "TX_ABORT");
        if (op == "PHASE") {
            require(request.size() == 3 && request[2] == "RESIZE", "PHASE requires RESIZE");
            if (!resize_phase()) { traffic_started_ns_ = Profile::now(); begin_resize_phase(); }
            return {"OK", "resize"};
        }
        if (op == "PROFILE") {
            require(request.size() == 3 && (request[2] == "READ" || request[2] == "RESET"), "PROFILE READ/RESET");
            return {"OK", Profile::dump(request[2] == "RESET")};
        }
        if (op == "READ_BATCH" || op == "CHECK_BATCH") return batch_read(request, op == "CHECK_BATCH");
        if (op == "READ_RANGES" || op == "CHECK_RANGES") return read_ranges(request);
        if (op == "REPAIR_RANGES") return repair_ranges(request);
        if (op == "RMW_BATCH") return batch_rmw(request);
        if (op == "PATCH_BATCH") return batch_patch(request);
        if (op == "PING") { require(request.size() == 2, "PING frame count"); return {"OK", id_}; }
        if (op == "BACKEND") {
            require(request.size() == 2, "BACKEND frame count");
            return {"OK", storage_backend()};
        }
        if (op == "CONFIG") {
            require(request.size() == 2, "CONFIG frame count");
            return {"OK", std::to_string(k_) + " " + std::to_string(m_) + " " + std::to_string(bytes_)};
        }
        if (op == "STATS") {
            require(request.size() == 2, "STATS frame count");
            auto stats = writers_.stats(); stats.pop_back();
            return {"OK", stats + ",\"memory\":" + memory_.stats() + ",\"traffic\":" + traffic_stats() + "}"};
        }
        if (op == "MEMORY") {
            require(request.size() == 2, "MEMORY frame count");
            return {"OK", memory_.stats(true)};
        }
        if (op == "LAYOUT") {
            require(request.size() == 3, "LAYOUT frame count");
            return {"OK", memory_.layout(request[2])};
        }
        if (op == "BUFFER_DELETE_BATCH") {
            require(request.size() == 3, "buffer delete batch frame count");
            auto names = unpack_record(request[2]);
            require(!names.empty() && names.size() <= 4096, "invalid buffer delete batch size");
            std::set<std::string> unique;
            // Reject a malformed batch before retiring any member.
            for (const auto& name : names) {
                valid_name(name);
                require(unique.insert(name).second, "duplicate buffer delete member");
            }
            for (const auto& name : names) {
                auto marker = "deleted-buffers/" + name;
                if (!memory_.contains(marker)) memory_.put(marker, "");
                memory_.erase("buffers/" + name);
            }
            return {"OK", ""};
        }
        if (op == "BUFFER_WRITE" || op == "BUFFER_READ" || op == "BUFFER_DELETE") {
            require(request.size() == (op == "BUFFER_WRITE" ? 4u : 3u), "buffer operation frame count");
            valid_name(request[2]);
            auto path = "buffers/" + request[2];
            if (op == "BUFFER_WRITE") {
                require(!memory_.contains("deleted-buffers/" + request[2]), "buffer version has been retired");
                require(!request[3].empty() && request[3].size() <= MAX_BLOCK, "invalid buffer size");
                memory_.put(path, request[3]);
                return {"OK", ""};
            }
            if (op == "BUFFER_DELETE") {
                auto marker = "deleted-buffers/" + request[2];
                if (!memory_.contains(marker)) memory_.put(marker, "");
                memory_.erase(path); // Idempotent after a lost acknowledgment while the process is alive.
                return {"OK", ""};
            }
            if (!memory_.contains(path)) return {"NOT_FOUND", "buffer replica missing"};
            return {"OK", memory_.get(path)};
        }
        if (op == "CLONE") {
            require(request.size() == 5, "CLONE requires source, destination and length");
            valid_name(request[2]); valid_name(request[3]);
            require(request[2] != request[3], "CLONE destination must be a new version");
            require(!memory_.contains("deleted/" + request[3]), "EC destination has been retired");
            std::istringstream header(request[4]);
            std::string extra;
            int length = 0;
            header >> length;
            require(header && !(header >> extra) && length >= 0 && length <= static_cast<int>(MAX_BLOCK), "invalid CLONE header");
            auto block = checked_block(request[2]);
            block.resize(length, '\0');
            memory_.put("blocks/" + request[3], block);
            return {"OK", ""};
        }
        if (op == "DELETE") {
            require(request.size() == 3, "DELETE requires an EC shard");
            valid_name(request[2]);
            require(request[2].size() >= 34 && request[2][32] == '_', "invalid EC shard name");
            auto marker = "deleted/" + request[2];
            if (!memory_.contains(marker)) memory_.put(marker, "");
            // Fence late writes from an aborted GC batch before acknowledging deletion.
            auto path = "blocks/" + request[2];
            auto bytes = memory_.contains(path) ? memory_.size(path) : 0;
            memory_.erase(path);
            return {"OK", std::to_string(bytes)};
        }
        if (op == "RMW_RESIZE") return rmw_resize(request);
        if (op == "PATCH_RESIZE") return patch_resize(request);
        if (op == "RMW") return rmw(request);
        if (op == "PATCH") return patch(request);
        if (op == "CHECK") {
            require(request.size() == 3, "CHECK frame count");
            if (memory_.direct_ranges()) {
                auto key = "blocks/" + request[2];
                auto length = memory_.size(key);
                require(length <= MAX_BLOCK, "invalid block length");
                if (!memory_.range_available(key, length, 0, length)) return {"NOT_FOUND", "partially repaired block"};
            }
            else checked_block(request[2]);
            return {"OK", ""};
        }
        require((op == "WRITE" && request.size() == 4) || (op == "READ" && request.size() == 3), "expected READ/WRITE");
        valid_name(request[2]);
        auto path = "blocks/" + request[2];
        if (op == "WRITE") {
            require(!memory_.contains("deleted/" + request[2]), "EC shard has been retired");
            require(request[3].size() <= MAX_BLOCK, "invalid block size");
            memory_.put(path, request[3]);
            return {"OK", ""};
        }
        if (!memory_.contains(path)) return {"NOT_FOUND", "block missing"};
        return {"OK", memory_.get(path)};
    }
};
int main() {
    try {
        Node node;
        Context context;
        serve_concurrent(context, env("BIND", "tcp://*:7000"),
            integer(env("NODE_REQUEST_THREADS", "8"), 1, 64),
            [&](const Frames& request) { return node.handle(request); },
            [](const Frames& r) {
                if (r.empty()) return false;
                const auto& op = r[0] == "MOSAIC" && r.size() == 5 ? r[2] : r[0];
                return op == "TX_RMW" || op == "TX_COMMIT" || op == "TX_ABORT";
            });
    } catch (const std::exception& e) { std::cerr << e.what() << std::endl; return 1; }
}
