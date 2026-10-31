// Included inside Node. service_mutex_ protects transaction/catalog state;
// independent chunk I/O uses persistent workers. Native batches are drained
// before any request buffers or directory entries may be changed/reclaimed.
struct BatchRange { size_t offset; std::string_view bytes; };
// These views refer to the owning request, which remains alive for this call.
// Outgoing async requests are serialized into owned strings before submission.
std::vector<BatchRange> batch_ranges(std::string_view raw, size_t length) {
    auto fields = unpack_record_views(raw);
    require(!fields.empty() && fields.size() % 2 == 0 && fields.size() <= 8192, "invalid fragment ranges");
    std::vector<BatchRange> ranges;
    ranges.reserve(fields.size() / 2);
    size_t end = 0;
    for (size_t r = 0; r < fields.size(); r += 2) {
        size_t offset = integer(std::string(fields[r]), 0, length - 1), count = fields[r + 1].size();
        require(offset >= end && count > 0 && count <= length - offset, "overlapping/out-of-bounds fragments");
        ranges.push_back({offset, fields[r + 1]});
        end = offset + count;
    }
    return ranges;
}
struct BatchMember {
    std::string name;
    int index = 0, k = 0, bytes = 0;
    std::vector<int> targets;
    Frames parity_names;
    std::vector<BatchRange> ranges;
};
Frames batch_read(const Frames& request, bool check) {
    require(memory_.batch_enabled() && request.size() == 3, "invalid batch read request");
    auto items = unpack_record(request[2]);
    size_t stride = 1;
    require(!items.empty() && items.size() % stride == 0 && items.size() / stride <= 4096, "invalid batch read size");
    Frames keys;
    for (size_t i = 0; i < items.size(); i += stride) { valid_name(items[i]); keys.push_back("blocks/" + items[i]); }
    size_t bytes = 0;
    for (const auto& key : keys) {
        auto length = memory_.size(key);
        require(length <= MAX_BATCH_READ_BYTES - bytes, "batch read exceeds 24 MiB of returned data");
        bytes += length;
    }
    auto values = memory_.get_many(keys);
    if (check) for (size_t i = 0; i < values.size(); ++i)
        require(values[i].size() >= 64 && values[i].size() <= MAX_BLOCK, "invalid batch block length");
    return {"OK", check ? "" : pack_record(values)};
}
// Native GET returns only requested fragment ranges, in logical object order.
Frames read_ranges(const Frames& request) {
    require(request.size() == 3, "invalid range read request");
    auto fields = unpack_record_views(request[2]);
    require(!fields.empty() && fields.size() % 4 == 0 && fields.size() <= 16384, "invalid read ranges");
    std::vector<NativeStore::ReadChunk> chunks;
    std::map<std::string, size_t> index;
    struct Part { size_t chunk, offset, length; };
    std::vector<Part> order;
    std::vector<size_t> sizes;
    size_t total = 0;
    for (size_t i = 0; i < fields.size(); i += 4) {
        std::string name(fields[i]); valid_name(name);
        auto key = "blocks/" + name;
        // Object-layout shards may be shorter than ISA-L's scratch minimum,
        // including a zero-byte data shard. This is the stored length.
        size_t length = integer(std::string(fields[i + 1]), 0, MAX_BLOCK);
        size_t offset = integer(std::string(fields[i + 2]), 0, length);
        size_t count = integer(std::string(fields[i + 3]), 0, length - offset);
        require(count <= MAX_VALUE - total, "read range exceeds 16 MiB");
        auto [it, added] = index.emplace(key, chunks.size());
        if (added) { chunks.push_back({key, length, {}}); sizes.push_back(0); }
        auto n = it->second;
        require(chunks[n].length == length, "inconsistent read chunk length");
        chunks[n].ranges.push_back({offset, count});
        order.push_back({n, sizes[n], count}); sizes[n] += count; total += count;
    }
    if (request[0] == "CHECK_RANGES") {
        std::string flags;
        // Preserve original fragment order, including repeated chunk IDs.
        for (size_t i = 0; i < fields.size(); i += 4) {
            auto key = "blocks/" + std::string(fields[i]);
            flags += memory_.range_available(key, std::stoull(std::string(fields[i + 1])),
                std::stoull(std::string(fields[i + 2])), std::stoull(std::string(fields[i + 3]))) ? '1' : '0';
        }
        return {"OK", flags};
    }
    auto parts = memory_.read_ranges_many(chunks);
    std::string result; result.reserve(total);
    for (const auto& part : order) result.append(parts[part.chunk], part.offset, part.length);
    return {"OK", std::move(result)};
}
Frames repair_ranges(const Frames& request) {
    require(request.size() == 3, "invalid range repair request");
    auto fields = unpack_record_views(request[2]);
    require(!fields.empty() && fields.size() % 4 == 0 && fields.size() <= 16384, "invalid repair ranges");
    struct Repair { std::string key; size_t length, offset; std::string_view bytes; };
    std::vector<Repair> repairs;
    std::map<std::string, size_t> sizes;
    size_t total = 0;
    for (size_t i = 0; i < fields.size(); i += 4) {
        std::string name(fields[i]); valid_name(name);
        require(!memory_.contains("deleted/" + name), "EC shard has been retired");
        size_t length = integer(std::string(fields[i + 1]), 64, MAX_BLOCK);
        size_t offset = integer(std::string(fields[i + 2]), 0, length);
        require(!fields[i + 3].empty() && fields[i + 3].size() <= length-offset
                && fields[i + 3].size() <= MAX_VALUE-total, "invalid repair range bounds");
        auto key = "blocks/" + name;
        auto [it, added] = sizes.emplace(key, length);
        require(added || it->second == length, "repair chunk length mismatch");
        repairs.push_back({key, length, offset, fields[i + 3]}); total += fields[i + 3].size();
    }
    for (const auto& r : repairs) memory_.repair_range(r.key, r.length, r.offset, r.bytes);
    return {"OK", ""};
}
Frames batch_patch(const Frames& request, bool* applied = nullptr) {
    Profile trace("parity");
    // PATCH_BATCH node tx packed([packed(name, chunk_bytes, packed(offset,delta,...)), ...])
    require(memory_.batch_enabled() && request.size() == 4, "invalid parity batch request");
    valid_name(request[2]);
    trace.id(request[2]);
    auto encoded = unpack_record_views(request[3]);
    require(!encoded.empty() && encoded.size() <= 4096, "invalid parity batch size");
    std::vector<std::vector<BatchRange>> ranges;
    std::vector<size_t> lengths;
    Frames keys;
    size_t bytes = 0, fragment_bytes = 0;
    for (const auto& item : encoded) {
        auto fields = unpack_record_views(item);
        require(fields.size() == 3, "invalid parity batch member");
        size_t length = integer(std::string(fields[1]), 64, MAX_BLOCK);
        require(length <= MAX_BATCH_CHUNK_BYTES - bytes, "parity batch exceeds 48 MiB of chunk data");
        bytes += length;
        auto fragments = batch_ranges(fields[2], length);
        for (const auto& range : fragments) {
            require(range.bytes.size() <= MAX_VALUE - fragment_bytes, "parity fragment payload exceeds 16 MiB");
            fragment_bytes += range.bytes.size();
        }
        std::string name(fields[0]);
        valid_name(name); keys.push_back("blocks/" + name);
        lengths.push_back(length); ranges.push_back(std::move(fragments));
    }
    require(std::set<std::string>(keys.begin(), keys.end()).size() == keys.size(), "duplicate parity block in batch");
    trace.mark("parse_validate");
    if (memory_.direct_ranges()) {
        std::vector<NativeStore::WriteChunk> chunks;
        chunks.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            chunks.push_back({keys[i], lengths[i], {}});
            for (const auto& range : ranges[i]) chunks.back().ranges.push_back({range.offset, range.bytes});
        }
        trace.mark("read_chunks");
        memory_.native().write_ranges_many(chunks, true);
        // Publish the journal state before profiling/reply allocations, which
        // may fail even though every XOR has already completed.
        if (applied) *applied = true;
        trace.mark("xor_delta"); trace.mark("write_chunks");
        return {"OK", ""};
    }
    auto blocks = memory_.get_many(keys);
    trace.mark("read_chunks");
    for (size_t i = 0; i < blocks.size(); ++i) {
        require(blocks[i].size() == lengths[i], "old batch parity length mismatch");
        for (const auto& range : ranges[i])
            for (size_t j = 0; j < range.bytes.size(); ++j) blocks[i][range.offset + j] ^= range.bytes[j];
    }
    trace.mark("xor_delta");
    memory_.put_many(keys, blocks);
    if (applied) *applied = true;
    trace.mark("write_chunks");
    return {"OK", ""};
}
Frames batch_rmw(const Frames& request) {
    Profile trace("data");
    // RMW_BATCH node tx packed([packed(name, header, packed(offset,data,...)), ...])
    // header: index K M B; M lines of target/name.
    require(memory_.batch_enabled() && request.size() == 4, "invalid RMW batch request");
    valid_name(request[2]);
    const bool transactional = request[0] == "TX_RMW";
    DataTransaction* transaction = nullptr;
    std::optional<BusyTransaction> busy;
    if (transactional) {
        require(memory_.direct_ranges(), "Mosaic transactions require native range storage");
        transaction = &data_transaction(request[2]);
        busy.emplace(transaction->busy, transactions_changed_);
        require(!transaction->aborted, "data transaction was aborted");
        if (transaction->ready) {
            require(transaction->terminal || transaction->payload == request[3], "transaction ID payload mismatch");
            return {"OK", ""};
        }
        require(!transaction->prepared, "incomplete transaction must be aborted before retry");
        transaction->payload = request[3];
    }
    trace.id(request[2]);
    auto encoded = unpack_record_views(request[3]);
    require(!encoded.empty() && encoded.size() <= 4096, "invalid RMW batch size");
    std::vector<BatchMember> members;
    members.reserve(encoded.size());
    Frames keys;
    std::set<std::pair<int, std::string>> parity_keys;
    size_t bytes = 0, fragment_bytes = 0;
    for (const auto& item : encoded) {
        auto fields = unpack_record_views(item);
        require(fields.size() == 3, "invalid RMW batch member");
        BatchMember member;
        member.name = fields[0]; valid_name(member.name);
        int m = 0;
        std::istringstream header{std::string(fields[1])};
        header >> member.index >> member.k >> m >> member.bytes;
        require(header && member.k >= 1 && member.k <= k_ && m == m_ && member.index >= 0 && member.index < member.k &&
                member.bytes >= 64 && member.bytes <= static_cast<int>(MAX_BLOCK), "invalid RMW batch configuration");
        require(static_cast<size_t>(member.bytes) <= MAX_BATCH_CHUNK_BYTES - bytes, "RMW batch exceeds 48 MiB of chunk data");
        bytes += member.bytes;
        std::set<int> targets;
        for (int j = 0; j < m_; ++j) {
            int target = -1;
            std::string name;
            header >> target >> name;
            require(header && target >= 0 && target < static_cast<int>(nodes_.size()) && std::to_string(target) != id_ &&
                    targets.insert(target).second && parity_keys.emplace(target, name).second, "invalid/overlapping parity batch route");
            valid_name(name);
            member.targets.push_back(target); member.parity_names.push_back(name);
        }
        std::string extra;
        require(!(header >> extra), "trailing RMW batch header");
        if (!members.empty()) require(member.targets == members[0].targets, "RMW batch must share parity node routes");
        member.ranges = batch_ranges(fields[2], member.bytes);
        for (const auto& range : member.ranges) {
            require(range.bytes.size() <= MAX_VALUE - fragment_bytes, "RMW fragment payload exceeds 16 MiB");
            fragment_bytes += range.bytes.size();
        }
        keys.push_back("blocks/" + member.name); members.push_back(std::move(member));
    }
    require(std::set<std::string>(keys.begin(), keys.end()).size() == keys.size(), "duplicate data block in RMW batch");
    trace.mark("parse_validate");
    FrameScratch::Scope delta_scope(batch_deltas_), parity_scope(batch_parity_);
    auto& deltas = delta_scope.frames();
    auto& parity = parity_scope.frames();
    Frames blocks;
    std::vector<NativeStore::WriteChunk> native_chunks;
    if (memory_.direct_ranges()) {
        native_chunks.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            native_chunks.push_back({keys[i], static_cast<size_t>(members[i].bytes), {}});
            for (const auto& range : members[i].ranges) native_chunks.back().ranges.push_back({range.offset, range.bytes});
        }
        memory_.native().delta_many(native_chunks, deltas);
        trace.mark("read_chunks");
    } else {
        blocks = memory_.get_many(keys);
        trace.mark("read_chunks");
        deltas.resize(members.size());
        for (size_t i = 0; i < members.size(); ++i) {
            const auto& member = members[i];
            require(blocks[i].size() == static_cast<size_t>(member.bytes), "old RMW batch length mismatch");
            size_t length = 0;
            for (const auto& range : member.ranges) length += range.bytes.size();
            auto& compact = deltas[i];
            compact.resize(length);
            size_t cursor = 0;
            for (const auto& range : member.ranges) {
                char* dst = compact.data() + cursor;
                const char* old = blocks[i].data() + range.offset;
                const char* src = range.bytes.data();
                for (size_t j = 0; j < range.bytes.size(); ++j)
                    dst[j] = old[j] ^ src[j];
                blocks[i].replace(range.offset, range.bytes.size(), range.bytes.data(), range.bytes.size());
                cursor += range.bytes.size();
            }
        }
    }
    trace.mark("build_delta");
    // Serialize directly into each final RPC payload. Avoid separate fragment,
    // member and outer-record copies of every encoded byte.
    Frames patches(m_), lengths;
    std::vector<Frames> offsets;
    std::vector<size_t> fragment_sizes, patch_sizes(m_, 0);
    for (const auto& member : members) {
        lengths.push_back(std::to_string(member.bytes));
        offsets.emplace_back();
        size_t size = 0;
        for (const auto& range : member.ranges) {
            offsets.back().push_back(std::to_string(range.offset));
            size += 8 + offsets.back().back().size() + range.bytes.size();
        }
        fragment_sizes.push_back(size);
        for (int j = 0; j < m_; ++j)
            patch_sizes[j] += 16 + member.parity_names[j].size() + lengths.back().size() + size;
    }
    for (int j = 0; j < m_; ++j) patches[j].reserve(patch_sizes[j]);
    for (size_t i = 0; i < members.size(); ++i) {
        const auto& member = members[i];
        auto& codec = codecs_[{member.k, member.bytes}];
        if (!codec) codec = std::make_unique<Codec>(member.k, m_, member.bytes);
        // Scratch is reused only after encoding/serialization has completed;
        // outgoing requests own their bytes. No transaction replay support.
        codec->parity_delta_into(member.index, deltas[i], parity);
        for (int j = 0; j < m_; ++j) {
            auto& patch = patches[j];
            append_record_length(patch, 12 + member.parity_names[j].size() + lengths[i].size() + fragment_sizes[i]);
            append_record_field(patch, member.parity_names[j]);
            append_record_field(patch, lengths[i]);
            append_record_length(patch, fragment_sizes[i]);
            size_t cursor = 0, r = 0;
            for (const auto& range : member.ranges) {
                append_record_field(patch, offsets[i][r++]);
                append_record_field(patch, std::string_view(parity[j]).substr(cursor, range.bytes.size()));
                cursor += range.bytes.size();
            }
        }
    }
    trace.mark("encode_pack_parity");
    if (transaction) {
        std::vector<UndoChunk> undo;
        for (size_t i = 0; i < members.size(); ++i) {
            undo.push_back({keys[i], static_cast<size_t>(members[i].bytes), {}});
            size_t offset = 0;
            for (const auto& range : members[i].ranges) {
                std::string old(range.bytes);
                for (size_t p = 0; p < old.size(); ++p) old[p] ^= deltas[i][offset+p];
                offset += old.size();
                undo.back().ranges.push_back({range.offset, std::move(old)});
            }
        }
        transaction->undo = std::move(undo);
        transaction->targets = members[0].targets;
        transaction->patches = patches;
        transaction->prepared = true;
    }
    NodeReplies pending(m_);
    for (int j = 0; j < m_; ++j) {
        int target = members[0].targets[j];
        Frames outgoing{transactional ? "TX_PATCH" : "PATCH_BATCH", std::to_string(target), request[2] + "_" + std::to_string(j)};
        outgoing.push_back(std::move(patches[j]));
        pending.add(writers_.submit(j, nodes_[target], std::move(outgoing), nullptr, target));
    }
    trace.mark("submit_parity");
    // Persistent parity workers run while the coordinator writes local data. Keep the
    // single-chunk inline path; multiple local chunks use the existing I/O pool.
    // Either side may fail after the other has written: drain both, never replay.
    std::exception_ptr failure;
    try {
        if (memory_.direct_ranges()) memory_.native().write_ranges_many(native_chunks);
        else memory_.put_many(keys, blocks);
    }
    catch (...) { failure = std::current_exception(); }
    trace.mark("write_chunks");
    { std::optional<NetworkWait> waiting;
      if (transactional) waiting.emplace();
      for (auto& task : pending) {
        try { task.get(); }
        catch (...) { if (!failure) failure = std::current_exception(); }
      }
    }
    trace.mark("wait_parity");
    if (failure) std::rethrow_exception(failure);
    if (transaction) transaction->ready = true;
    return {"OK", ""};
}
