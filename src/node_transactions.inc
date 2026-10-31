// Included inside Node, after the batch implementation.
struct UndoRange { size_t offset; std::string bytes; };
struct UndoChunk { std::string key; size_t length; std::vector<UndoRange> ranges; };
struct DataTransaction {
    bool terminal = false, prepared = false, ready = false, aborted = false, busy = false;
    std::vector<UndoChunk> undo;
    std::vector<int> targets;
    Frames patches;
    std::string payload;
};
struct ParityTransaction {
    bool terminal = false, applied = false, aborted = false, busy = false;
    std::string payload;
};
TransactionJournal<DataTransaction> data_transactions_;
TransactionJournal<ParityTransaction> parity_transactions_;
struct BusyTransaction {
    bool& busy;
    std::condition_variable& changed;
    BusyTransaction(bool& b, std::condition_variable& c) : busy(b), changed(c) { busy = true; }
    ~BusyTransaction() { busy = false; changed.notify_all(); }
};
DataTransaction& data_transaction(const std::string& id) {
    for (;;) {
        auto& tx = data_transactions_.get(id);
        if (!tx.busy) return tx;
        transactions_changed_.wait(*service_lock_);
        // Revalidate sequence after waiting: another request may have advanced
        // this stream when the original transaction became terminal.
    }
}

Frames transaction_patch(const Frames& r) {
    require(r.size() == 4 && memory_.direct_ranges(), "transactional parity requires native range storage");
    valid_name(r[2]);
    auto& tx = parity_transactions_.get(r[2]);
    require(!tx.aborted, "parity transaction was aborted");
    if (tx.applied) {
        require(tx.terminal || tx.payload == r[3], "transaction ID payload mismatch");
        return {"OK", ""};
    }
    tx.payload = r[3]; // Retain encoded XOR deltas before touching parity.
    batch_patch(r, &tx.applied);
    return {"OK", ""};
}
Frames transaction_parity_finish(const Frames& r, bool abort) {
    require(r.size() == 3 && memory_.direct_ranges(), "invalid parity transaction finish");
    valid_name(r[2]);
    auto& tx = parity_transactions_.get(r[2]);
    if (tx.terminal) {
        require(tx.aborted == abort, "conflicting parity transaction decision");
        return {"OK", ""};
    }
    if (abort && tx.applied) {
        bool undone = false;
        try { batch_patch({"PATCH_BATCH", id_, r[2], tx.payload}, &undone); }
        catch (...) {
            if (!undone) throw;
            // A post-write profiling/reply failure must not undo the delta a
            // second time on retry. The rollback itself already completed.
        }
    }
    if (!abort) require(tx.applied, "cannot commit unapplied parity");
    tx.aborted = abort; tx.terminal = true;
    std::string().swap(tx.payload);
    return {"OK", ""};
}
Frames transaction_data_finish(const Frames& r, bool abort) {
    require(r.size() == 3 && memory_.direct_ranges(), "invalid data transaction finish");
    valid_name(r[2]);
    auto& tx = data_transaction(r[2]);
    BusyTransaction busy(tx.busy, transactions_changed_);
    if (tx.terminal) {
        require(tx.aborted == abort, "conflicting data transaction decision");
        return {"OK", ""};
    }
    if (!abort) require(tx.ready, "cannot commit incomplete data transaction");
    // Wait for every parity node, including nodes whose apply reply was lost.
    // An abort received before apply is a tombstone that fences a late apply.
    NodeReplies pending(m_);
    for (size_t j = 0; j < tx.targets.size(); ++j) {
        const auto node = tx.targets[j];
        pending.add(writers_.submit(j, nodes_[node],
            {abort ? "TX_PARITY_ABORT" : "TX_PARITY_COMMIT", std::to_string(node), r[2]+"_"+std::to_string(j)}, nullptr, node));
    }
    std::exception_ptr failure;
    { NetworkWait waiting;
      for (auto& task : pending) { try { task.get(); } catch (...) { if (!failure) failure = std::current_exception(); } } }
    if (failure) std::rethrow_exception(failure);
    if (abort && tx.prepared) {
        std::vector<NativeStore::WriteChunk> chunks;
        for (const auto& u : tx.undo) {
            chunks.push_back({u.key, u.length, {}});
            for (const auto& range : u.ranges) chunks.back().ranges.push_back({range.offset, range.bytes});
        }
        memory_.native().write_ranges_many(chunks);
    }
    tx.aborted = abort; tx.terminal = true;
    std::vector<UndoChunk>().swap(tx.undo); Frames().swap(tx.patches);
    std::string().swap(tx.payload);
    return {"OK", ""};
}
