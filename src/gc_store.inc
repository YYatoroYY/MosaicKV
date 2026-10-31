    // Store members. Foreground requests hold mutex_; GC holds it only for snapshots,
    // journal/catalog publication and statistics, never for planning, bulk I/O or encoding.
    static uint64_t gc_capacity() {
        const auto value = env("GC_NODE_CAPACITY_BYTES", "0");
        require(!value.empty() && value.find_first_not_of("0123456789") == std::string::npos, "invalid GC_NODE_CAPACITY_BYTES");
        size_t end = 0;
        auto bytes = std::stoull(value, &end);
        require(end == value.size() && bytes <= (uint64_t(1) << 60), "invalid GC_NODE_CAPACITY_BYTES");
        return bytes;
    }
    std::map<StripeId, uint32_t> live_masks() const {
        std::map<StripeId, uint32_t> masks;
        for (const auto& [key, object] : catalog_.keys)
            for (const auto& extent : object.extents) {
                auto it = catalog_.stripes.find(extent.stripe);
                if (it == catalog_.stripes.end()) continue;
                const auto& stripe = it->second;
                if (!extent.length) masks[extent.stripe] |= uint32_t(1) << (extent.offset / stripe.block_bytes);
                else for (size_t i = extent.offset / stripe.block_bytes; i <= (extent.offset + extent.length - 1) / stripe.block_bytes; ++i)
                    masks[extent.stripe] |= uint32_t(1) << i;
            }
        return masks;
    }
    struct GcUsage {
        uint64_t invalid = 0, total = 0, peak_node = 0, logical = 0, ec_logical = 0;
        uint64_t parity = 0, retired = 0, local_buffers = 0, replica_buffers = 0;
        size_t dirty = 0;
    };
    GcUsage gc_usage() const {
        GcUsage usage;
        std::vector<uint64_t> bytes(nodes_.size());
        if (catalog_.object_layout) {
            usage.logical = logical_live_bytes_;
            usage.ec_logical = gc_index_.logical;
            usage.total = gc_index_.total; usage.invalid = gc_index_.invalid;
            usage.parity = gc_index_.parity;
            usage.dirty = gc_index_.dirty_count;
            bytes = gc_index_.node_bytes;
        } else {
            auto masks = live_masks(); // Legacy packed-block layout only.
            for (const auto& [key, object] : catalog_.keys) {
                usage.logical += object.size();
                for (const auto& extent : object.extents)
                    if (catalog_.stripes.count(extent.stripe)) usage.ec_logical += extent.length;
            }
            for (const auto& [id, stripe] : catalog_.stripes) {
                int dead = stripe.data_blocks - __builtin_popcount(masks[id]);
                usage.invalid += static_cast<uint64_t>(dead) * stripe.block_bytes;
                usage.total += static_cast<uint64_t>(stripe.data_blocks) * stripe.block_bytes;
                usage.dirty += dead != 0;
                usage.parity += static_cast<uint64_t>(m_) * stripe.block_bytes;
                for (int i = 0; i < width(stripe); ++i) bytes[node_for(stripe, i)] += stripe.block_bytes;
            }
        }
        for (const auto& [version, value] : local_buffers_) usage.local_buffers += value.size();
        for (const auto& [version, old] : catalog_.retired_stripes) {
            for (int i = 0; i < old.count; ++i) {
                usage.retired += old.stored(i);
                bytes[(old.start + i) % nodes_.size()] += old.stored(i);
            }
        }
        for (const auto& [id, buffer] : catalog_.buffers)
            for (int node : buffer.replicas) { bytes[node] += buffer.bytes; usage.replica_buffers += buffer.bytes; }
        for (auto size : bytes) usage.peak_node = std::max(usage.peak_node, size);
        return usage;
    }
    std::string gc_stats() const {
        const auto usage = gc_usage();
        std::ostringstream out;
        out << "{\"storage_mode\":\"memory\",\"storage_backend\":\"" << storage_backend() << "\",\"enabled\":" << (strategy_ == "fastgc" ? "true" : "false")
            << ",\"metadata_mode\":\"embedded_stripe_accounting\""
            << ",\"padding_storage\":\"implicit_zero\""
            << ",\"candidate_owner_visits\":" << gc_candidate_owner_visits_
            << ",\"max_snapshot_stripes\":" << gc_snapshot_max_
            << ",\"proxy_rss_bytes\":" << process_rss_bytes()
            << ",\"implementation\":\"fastgc-inspired-full-reencode\",\"paused\":" << (gc_paused_ ? "true" : "false")
            << ",\"busy\":" << (gc_busy_ ? "true" : "false")
            << ",\"suspended\":" << (gc_suspended_ ? "true" : "false")
            << ",\"phase\":\"" << gc_phase_ << "\",\"requested\":" << (gc_forced_ ? "true" : "false")
            << ",\"interval_ms\":" << gc_interval_ << ",\"start_percent\":" << gc_start_percent_
            << ",\"every_updates\":" << gc_every_updates_
            << ",\"counted_updates\":" << gc_counted_updates_
            << ",\"update_triggers\":" << gc_update_triggers_ << ",\"pending_update_rounds\":" << gc_update_pending_
            << ",\"coalesced_update_triggers\":" << gc_update_coalesced_
            << ",\"started_update_rounds\":" << gc_update_started_
            << ",\"completed_update_rounds\":" << gc_update_completed_
            << ",\"drain_batches\":" << gc_drain_batches_
            << ",\"drain_on_update_trigger\":" << (gc_every_updates_ ? "true" : "false")
            << ",\"stop_percent\":" << gc_stop_percent_ << ",\"batch_stripes\":" << gc_batch_
            << ",\"node_capacity_bytes\":" << gc_capacity_ << ",\"space_start_percent\":" << gc_space_start_
            << ",\"space_stop_percent\":" << gc_space_stop_ << ",\"peak_managed_node_bytes\":" << usage.peak_node
            << ",\"invalid_data_bytes\":" << usage.invalid << ",\"total_data_bytes\":" << usage.total
            << ",\"logical_live_bytes\":" << usage.logical
            << ",\"live_padding_bytes\":" << usage.total - usage.invalid - usage.ec_logical
            << ",\"space_peaks\":{\"scope\":\"committed_active_data_since_reset\""
            << ",\"padding_bytes\":" << gc_space_peaks_.padding
            << ",\"invalid_data_bytes\":" << gc_space_peaks_.invalid
            << ",\"padding_plus_invalid_bytes\":" << gc_space_peaks_.waste
            << ",\"live_bytes_at_padding_peak\":" << gc_space_peaks_.live_at_padding
            << ",\"live_bytes_at_waste_peak\":" << gc_space_peaks_.live_at_waste
            << ",\"observations\":" << gc_space_peaks_.observations << '}'
            << ",\"amplification_peak\":{\"scope\":\"committed_active_node_payload_since_reset\""
            << ",\"reference\":\"(1+m/k)*logical_live_bytes_at_same_commit\""
            << ",\"k\":" << k_ << ",\"m\":" << m_
            << ",\"defined\":" << (gc_amplification_peak_.live ? "true" : "false")
            << ",\"normalized_space_amplification\":" << gc_amplification_peak_.normalized(k_, m_)
            << ",\"payload_bytes\":" << gc_amplification_peak_.payload
            << ",\"logical_live_bytes\":" << gc_amplification_peak_.live
            << ",\"data_bytes\":" << gc_amplification_peak_.data
            << ",\"parity_bytes\":" << gc_amplification_peak_.parity
            << ",\"buffer_replica_bytes\":" << gc_amplification_peak_.replicas
            << ",\"observations\":" << gc_amplification_peak_.observations
            << ",\"observation_at_peak\":" << gc_amplification_peak_.observation_at_peak << '}'
            << ",\"active_parity_bytes\":" << usage.parity
            << ",\"retired_ec_catalog_bytes\":" << usage.retired
            << ",\"pending_invalid_catalog_bytes\":" << usage.invalid + usage.retired
            << ",\"proxy_buffer_bytes\":" << usage.local_buffers
            << ",\"buffer_replica_bytes\":" << usage.replica_buffers
            << ",\"active_node_payload_bytes\":" << usage.total + usage.parity + usage.replica_buffers
            << ",\"garbage_percent\":" << (usage.total ? 100.0 * usage.invalid / usage.total : 0)
            << ",\"dirty_stripes\":" << usage.dirty << ",\"retired_stripes\":" << catalog_.retired_stripes.size()
            << ",\"cycles\":" << gc_cycles_ << ",\"commits\":" << gc_commits_ << ",\"conflicts\":" << gc_conflicts_
            << ",\"failures\":" << gc_failures_ << ",\"no_plan\":" << gc_no_plan_
            << ",\"source_stripes\":" << gc_sources_ << ",\"output_stripes\":" << gc_outputs_
            << ",\"net_bytes_saved\":" << gc_net_bytes_ << ",\"reclaimed_bytes\":" << gc_reclaimed_bytes_
            << ",\"local_clones\":" << gc_cloned_ << ",\"read_payload_bytes\":" << gc_read_bytes_
            << ",\"write_payload_bytes\":" << gc_write_bytes_ << ",\"cleanup_failures\":" << gc_cleanup_failures_ << '}';
        return out.str();
    }
    void recover_gc_intent() {
        if (gc_intent_.empty()) return;
        // Published outputs are removed from the intent at commit. Remaining
        // entries are unpublished/partial outputs and can be retired directly.
        install_records(catalog_.retired_stripes, gc_intent_);
        gc_intent_.clear();
    }
    std::future<Frames> gc_submit(int node, Frames request) {
        request.insert(request.begin() + 1, std::to_string(node));
        return node_requests_->submit(node, std::move(request), timeout_, gc_traffic_.node(node));
    }
    static std::string gc_reply(std::future<Frames>& task) {
        auto reply = task.get();
        if (reply.size() != 2 || reply[0] != "OK")
            throw Error("UNAVAILABLE", reply.size() == 2 ? reply[1] : "invalid node response");
        return std::move(reply[1]);
    }
    void cleanup_gc_stripes() {
        std::map<StripeId, RetiredStripe> work;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            if (rmw_active_) return;
            for (const auto& item : catalog_.retired_stripes) {
                work.insert(item);
                if (work.size() >= static_cast<size_t>(gc_batch_)) break;
            }
        }
        std::vector<std::string> done;
        uint64_t reclaimed = 0, failures = 0;
        for (const auto& [version, old] : work) {
            PendingReplies tasks(old.count);
            for (int i = 0; i < old.count; ++i)
                tasks.add(gc_submit((old.start + i) % nodes_.size(), {"DELETE", version + "_" + std::to_string(i)}));
            bool success = true;
            for (auto& task : tasks) try { reclaimed += std::stoull(gc_reply(task)); } catch (const std::exception&) { success = false; }
            if (success) done.push_back(version);
            else ++failures;
        }
        std::lock_guard<std::mutex> guard(mutex_);
        gc_cleanup_failures_ += failures;
        gc_reclaimed_bytes_ += reclaimed;
        if (!done.empty()) {
            for (const auto& version : done) catalog_.retired_stripes.erase(version);
        }
    }
    void gc_round(bool forced) {
        GcPlan plan;
        std::vector<GcCandidate> candidates;
        std::map<StripeId, Stripe> sources;
        std::map<StripeId, uint64_t> epochs;
        std::vector<std::pair<StripeId, Stripe>> outputs;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            if (strategy_ != "fastgc" || !resize_phase() || (gc_paused_ && !forced) || !catalog_.object_layout || rmw_active_) return;
            ++gc_cycles_;
            ObjectEdit empty_edit;
            uint64_t empty_bytes = 0, empty_count = 0;
            for (const auto& id : gc_index_.empty) {
                const auto& stripe = catalog_.stripes.at(id);
                empty_edit.stripes.insert(id);
                empty_bytes += stored_total(stripe);
                if (++empty_count >= static_cast<uint64_t>(gc_batch_)) break;
            }
            if (empty_count) {
                commit_object(std::move(empty_edit));
                ++gc_commits_; gc_sources_ += empty_count; gc_net_bytes_ += empty_bytes;
            }
            double garbage = gc_index_.total ? 100.0 * gc_index_.invalid / gc_index_.total : 0;
            // Capacity accounting includes retired shards and buffer replicas;
            // the usual garbage-ratio trigger needs only cached scalar totals.
            double space = gc_capacity_ ? 100.0 * gc_usage().peak_node / gc_capacity_ : 0;
            if (gc_active_ && garbage <= gc_stop_percent_ && space <= gc_space_stop_) gc_active_ = false;
            if (garbage >= gc_start_percent_ || (gc_capacity_ && space >= gc_space_start_)) gc_active_ = true;
            if (!forced && !gc_active_) return;
            gc_candidate_count_ = gc_index_.dirty.size();
            size_t count = std::min(gc_candidate_count_, static_cast<size_t>(gc_batch_));
            candidates.reserve(count);
            auto cursor = gc_index_.dirty.upper_bound(gc_cursor_);
            for (size_t n = 0; n < count; ++n) {
                if (cursor == gc_index_.dirty.end()) cursor = gc_index_.dirty.begin();
                const auto& id = *cursor++;
                const auto& stripe = catalog_.stripes.at(id);
                GcCandidate candidate{id, stripe.live_mask,
                    stored_total(stripe), {}};
                for (const auto* owner : stripe.owners) {
                    ++gc_candidate_owner_visits_;
                    const auto& object = catalog_.keys.at(*owner);
                    int index = object.extents.at(0).offset;
                    candidate.objects.push_back({*owner, id, index, node_for(stripe, index), object.size()});
                }
                candidates.push_back(std::move(candidate));
                gc_cursor_ = id;
            }
            gc_snapshot_max_ = std::max(gc_snapshot_max_, candidates.size());
            for (const auto& candidate : candidates) {
                sources[candidate.id] = catalog_.stripes.at(candidate.id);
                epochs[candidate.id] = catalog_.stripes.at(candidate.id).epoch;
            }
            gc_phase_ = "planning";
        }
        plan = plan_gc(candidates, k_, m_, nodes_.size());
        {
            std::lock_guard<std::mutex> guard(mutex_);
            if (plan.groups.empty()) { ++gc_no_plan_; gc_active_ = false; gc_phase_ = "idle"; return; }
            bool conflict = rmw_active_;
            std::set<StripeId> selected(plan.sources.begin(), plan.sources.end());
            for (const auto& id : selected)
                conflict |= !catalog_.stripes.count(id) || catalog_.stripes.at(id).epoch != epochs.at(id);
            if (conflict) { ++gc_conflicts_; gc_phase_ = "idle"; return; }
            for (auto it = sources.begin(); it != sources.end();)
                if (!selected.count(it->first)) it = sources.erase(it); else ++it;
            std::map<StripeId, RetiredStripe> intent;
            for (const auto& group : plan.groups) {
                Stripe stripe;
                stripe.version = StripeId::next(); stripe.start = group.start; stripe.data_blocks = k_; stripe.block_bytes = group.bytes;
                for (const auto& object : group.objects) stripe.lengths.push_back(object.length);
                intent.emplace(stripe.version, retired(stripe));
                outputs.emplace_back(StripeId::next(), std::move(stripe));
            }
            gc_intent_ = std::move(intent);
            gc_phase_ = "reading";
        }
        uint64_t read_bytes = 0, write_bytes = 0, clones = 0;
        try {
            for (size_t g = 0; g < plan.groups.size(); ++g) {
                const auto& group = plan.groups[g];
                auto& stripe = outputs[g].second;
                std::string data;
                std::vector<bool> clone_ok;
                PendingReplies reads(group.objects.size());
                for (const auto& object : group.objects)
                    reads.add(gc_submit(object.node, {"READ", block_name(sources.at(object.source), object.index)}));
                auto reply = reads.begin();
                for (const auto& object : group.objects) {
                    const auto& old = sources.at(object.source);
                    std::string block;
                    bool direct = false;
                    auto& pending = *reply++;
                    try {
                        block = gc_reply(pending);
                        read_bytes += block.size();
                        direct = block.size() == stored_bytes(old, object.index);
                    } catch (const std::exception&) { }
                    if (!direct) {
                        block = recover_shard_range(old, object.index, 0, object.length, &gc_traffic_, &read_bytes);
                    }
                    block.resize(object.length);
                    block.resize(group.bytes, '\0');
                    data += block;
                    clone_ok.push_back(direct);
                }
                Codec codec(k_, m_, stripe.block_bytes); // Never share the foreground codec cache.
                auto blocks = codec.encode(data);
                { std::lock_guard<std::mutex> guard(mutex_); gc_phase_ = "writing"; }
                PendingReplies tasks(k_);
                for (int i = 0; i < k_; ++i) {
                    const auto& object = group.objects[i];
                    bool local = clone_ok[i] && node_for(stripe, i) == object.node;
                    if (local) {
                        ++clones;
                        const auto& old = sources.at(object.source);
                        tasks.add(gc_submit(object.node, {"CLONE", block_name(old, object.index), block_name(stripe, i),
                            std::to_string(stored_bytes(stripe, i))}));
                    } else {
                        blocks[i].resize(stored_bytes(stripe, i));
                        write_bytes += blocks[i].size();
                        tasks.add(gc_submit(node_for(stripe, i), {"WRITE", block_name(stripe, i), blocks[i]}));
                    }
                }
                PendingReplies parities(m_);
                for (int j = 0; j < m_; ++j) {
                    int i = k_ + j, node = node_for(stripe, i);
                    blocks[i].resize(stored_bytes(stripe, i));
                    write_bytes += blocks[i].size();
                    parities.add(buffer_writers_.submit(j, nodes_[node], {"WRITE", std::to_string(node), block_name(stripe, i), blocks[i]}, gc_traffic_.node(node), node));
                }
                std::string error;
                for (auto& task : tasks) try { gc_reply(task); } catch (const std::exception& e) { error = e.what(); }
                for (auto& task : parities) try { task.get(); } catch (const std::exception& e) { error = e.what(); }
                if (!error.empty()) throw Error("UNAVAILABLE", error);
            }
            std::lock_guard<std::mutex> guard(mutex_);
            bool conflict = rmw_active_;
            for (const auto& [id, old] : sources)
                conflict |= !catalog_.stripes.count(id) || catalog_.stripes.at(id).epoch != epochs.at(id);
            if (conflict) ++gc_conflicts_;
            else {
                ObjectEdit edit;
                edit.stripes.insert(plan.sources.begin(), plan.sources.end());
                for (size_t g = 0; g < outputs.size(); ++g) {
                    auto& [id, stripe] = outputs[g];
                    for (int i = 0; i < k_; ++i) {
                        const auto& object = plan.groups[g].objects[i];
                        edit.keys.insert(object.key);
                        edit.next.keys.emplace(object.key, Object{{{id, static_cast<size_t>(i), object.length}}});
                    }
                    edit.next.stripes.emplace(id, stripe);
                }
                commit_object(std::move(edit));
                for (const auto& [id, stripe] : outputs) gc_intent_.erase(stripe.version);
                ++gc_commits_; gc_sources_ += sources.size(); gc_outputs_ += outputs.size();
                gc_net_bytes_ += plan.old_bytes - plan.new_bytes;
            }
            gc_cloned_ += clones; gc_read_bytes_ += read_bytes; gc_write_bytes_ += write_bytes;
            recover_gc_intent();
            gc_phase_ = "idle";
        } catch (...) {
            // In-memory intent accounts for every planned output, including partial writes.
            // Cleanup runs only after all network futures have finished/unwound.
            std::lock_guard<std::mutex> guard(mutex_);
            gc_cloned_ += clones; gc_read_bytes_ += read_bytes; gc_write_bytes_ += write_bytes;
            recover_gc_intent();
            gc_phase_ = "idle";
            // A foreground update may retire a source during READ/CLONE,
            // before we reach publication. This is a snapshot conflict,
            // just like an epoch mismatch at commit, not a storage failure.
            bool conflict = rmw_active_;
            for (const auto& [id, old] : sources)
                conflict |= !catalog_.stripes.count(id) || catalog_.stripes.at(id).epoch != epochs.at(id);
            if (conflict) { ++gc_conflicts_; return; }
            throw;
        }
    }
    void gc_drain() {
        // A trigger runs as many bounded batches as can make progress. When a
        // candidate window has no plan, scan the other windows before stopping.
        // Concurrent mutations/conflicts invalidate that no-progress sweep.
        size_t scanned = 0;
        while (true) {
            uint64_t commits, conflicts, mutations, reclaimed;
            {
                std::unique_lock<std::mutex> guard(mutex_);
                gc_cv_.wait(guard, [&] {
                    return gc_stop_ || gc_paused_ || gc_suspended_ || !rmw_active_;
                });
                if (gc_stop_ || gc_paused_ || gc_suspended_) return;
                commits = gc_commits_; conflicts = gc_conflicts_;
                mutations = updates_ + puts_ + deletes_;
                reclaimed = gc_reclaimed_bytes_;
            }
            gc_round(true);
            cleanup_gc_stripes();
            {
                std::lock_guard<std::mutex> guard(mutex_);
                ++gc_drain_batches_;
                if (gc_stop_ || gc_paused_ || gc_suspended_) return;
                if (rmw_active_) continue;
                if (commits != gc_commits_ || conflicts != gc_conflicts_ ||
                    mutations != updates_ + puts_ + deletes_) {
                    scanned = 0;
                    continue;
                }
                if (!catalog_.retired_stripes.empty() && reclaimed != gc_reclaimed_bytes_) continue;
                scanned += std::min(gc_candidate_count_, static_cast<size_t>(gc_batch_));
                if (scanned >= gc_candidate_count_) return;
            }
        }
    }
    void gc_loop() {
        pthread_setname_np(pthread_self(), "ec-gc");
        while (true) {
            bool forced = false, update_round = false;
            {
                std::unique_lock<std::mutex> guard(mutex_);
                if (gc_every_updates_ && strategy_ == "fastgc") {
                    gc_cv_.wait(guard, [&] {
                        return gc_stop_ || (!gc_suspended_ && (gc_forced_ ||
                            (!gc_paused_ && gc_update_pending_ && !rmw_active_)));
                    });
                } else {
                    gc_cv_.wait_for(guard, std::chrono::milliseconds(gc_interval_), [&] { return gc_stop_ || gc_wake_; });
                }
                if (gc_stop_) return;
                if (gc_suspended_) { gc_wake_ = false; continue; }
                gc_busy_ = true;
                gc_wake_ = false;
                forced = gc_forced_; gc_forced_ = false;
                if (gc_every_updates_ && gc_update_pending_ && !gc_paused_ && !rmw_active_) {
                    gc_update_pending_ = 0;
                    ++gc_update_started_;
                    update_round = true;
                    forced = true;
                }
            }
            try {
                {
                    std::lock_guard<std::mutex> guard(mutex_);
                    // Retire unpublished outputs before a new plan replaces their
                    // process-local intent after an interrupted batch.
                    if (!gc_intent_.empty()) recover_gc_intent();
                }
                if (update_round) gc_drain();
                else {
                    gc_round(forced);
                    if (strategy_ == "fastgc") cleanup_gc_stripes();
                }
            } catch (const std::exception& e) {
                std::cerr << "background GC: " << e.what() << std::endl;
                std::lock_guard<std::mutex> guard(mutex_);
                ++gc_failures_; gc_phase_ = "idle"; gc_wake_ = false;
            }
            {
                std::lock_guard<std::mutex> guard(mutex_);
                if (update_round) ++gc_update_completed_;
                gc_busy_ = false;
                gc_cv_.notify_all();
            }
        }
    }
