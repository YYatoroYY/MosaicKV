#pragma once
#include "mosaic_allocator.h"
#include "remote_holes.h"
#include "compact_object.h"
#include "compact_stripes.h"
#include "mosaic_load.h"
#include "codec.h"
#include "request_workers.h"
#include "profile.h"
#include "record_view.h"
#include "concurrent_index.h"
#include "stripe_barrier.h"
#include <list>
#include <memory>
#include <mutex>

namespace ec {
// Independent of the object layouts used by KVMD/fastgc. All metadata is RAM.
class MosaicStore {
    using Tree = mosaic::RemoteHoles;
    using Hole = mosaic::Hole;
    using Stripe = mosaic::CompactStripes::Stripe;
    using Object = mosaic::CompactObject;
    struct ReadEpoch {};
    struct Version { Object object; std::shared_ptr<ReadEpoch> epoch; };
    struct RetiredVersion {
        std::shared_ptr<ReadEpoch> epoch;
        int group;
        std::vector<Hole> fragments;
        size_t released = 0;
    };
    struct Action {
        Hole fragment;
        int coefficient;
        std::string bytes;
    };
    struct Pending {
        std::string key;
        int group = -1;
        int release_group = -1;
        std::optional<mosaic::Plan> allocation;
        bool reserved = false, io_complete = false;
        bool aborting = false, published = false;
        size_t abort_released = 0;
        std::vector<Hole> releases;
        size_t released = 0; // Resume after an allocation failure during hole coalescing.
        std::shared_ptr<const Version> publication;
        std::list<RetiredVersion> retirement; // Preallocated node for noexcept splicing.
        std::vector<uint64_t> stripe_ids;
        bool barrier = false;
        int stream = -1;
        std::vector<Action> actions;
        Frames batch_request; // Retained until the commit/abort decision is acknowledged.
        int node = -1;
        bool erase = false, update = false, resize = false;
        bool reallocated = false, node_migration = false, coefficient_change = false;
        bool object_update = false, rebalance = false;
        bool space_fallback = false;
        bool append = false, append_retained = false;
        uint64_t parity_bytes = 0;
        int parity_start = -1;
    };
    struct Replies {
        std::vector<std::future<Frames>> jobs;
        ~Replies() { for (auto& job : jobs) if (job.valid()) job.wait(); }
    };
    int k_, m_, timeout_;
    int stats_timeout_ = integer(env("STATS_TIMEOUT_MS", "10000"), 10, 60000);
    mosaic::Parameters parameters_;
    Codec codec_;
    Context context_;
    std::vector<std::string> nodes_;
    mosaic::Layout layout_;
    RequestWorkers workers_;
    std::string placement_ = env("MOSAIC_PLACEMENT", "parity_bandwidth");
    std::string space_fallback_ = env("MOSAIC_SPACE_FALLBACK", "family_load");
    uint64_t space_fallback_attempts_ = 0, space_fallback_probes_ = 0;
    uint64_t space_fallback_load_rejections_ = 0, space_fallback_selections_ = 0, space_fallback_updates_ = 0;
    struct SpaceFallbackDecision {
        int from = -1, to = -1;
        uint64_t source_bytes = 0, target_bytes = 0, update_number = 0;
    } last_space_fallback_;
    bool append_enabled_ = integer(env("MOSAIC_APPEND", "0"), 0, 1);
    uint64_t appends_ = 0, append_retained_ = 0, append_fallbacks_ = 0;
    uint64_t load_started_ms_ = now_ms();
    mosaic::CumulativeBandwidth parity_load_, data_load_;
    uint64_t rebalance_every_ = integer(env("MOSAIC_REBALANCE_EVERY", "1"), 1, 1000000000);
    uint64_t object_updates_ = 0, rebalance_events_ = 0;
    std::vector<Tree> trees_;
    mosaic::CompactStripes stripes_;
    std::vector<std::vector<uint64_t>> virgin_;
    std::optional<std::pair<uint64_t, int>> materializing_;
    uint64_t materialized_data_chunks_ = 0;
    // One random 128-bit seed for this RAM-only store; adding the stripe ID
    // derives its unique version without keeping a UUID string per stripe.
    static std::array<uint64_t, 2> stripe_seed() {
        auto id = uuid();
        return {std::stoull(id.substr(0, 16), nullptr, 16), std::stoull(id.substr(16), nullptr, 16)};
    }
    const std::array<uint64_t, 2> stripe_seed_ = stripe_seed();
    using Index = ConcurrentIndex<Version, Pending>;
    Index objects_;
    using Entry = typename Index::Entry;
    struct Operation {
        std::shared_ptr<Entry> entry;
        std::unique_lock<std::mutex>* lock;
    };
    inline static thread_local Operation* operation_ = nullptr;
    struct OperationScope {
        Operation* previous;
        explicit OperationScope(Operation& op) : previous(operation_) { operation_ = &op; }
        ~OperationScope() { operation_ = previous; }
    };
    struct Unlocked {
        Unlocked() { operation_->lock->unlock(); }
        ~Unlocked() { operation_->lock->lock(); }
    };
    std::unique_ptr<Pending>& pending() { return operation_->entry->pending; }
    std::list<RetiredVersion> deferred_;
    size_t pending_count_ = 0, live_keys_ = 0, active_reads_ = 0;
    uint64_t reader_retained_bytes_ = 0, reclaimed_fragments_ = 0;
    size_t active_writes_ = 0, max_active_writes_ = 0, max_active_reads_ = 0;
    StripeBarrier parity_barrier_;
    std::map<std::string, int> retired_; // Aborted fresh zero-stripe versions awaiting fences.
    std::vector<std::pair<int, uint64_t>> retired_holes_;
    struct Stream { std::string session = uuid(); uint64_t sequence = 0; bool busy = false; };
    std::array<Stream, 64> streams_;
    std::string next_transaction() {
        auto& p = *pending();
        if (p.stream < 0) for (size_t i = 0; i < streams_.size(); ++i) if (!streams_[i].busy) {
            p.stream = i; streams_[i].busy = true; break;
        }
        require(p.stream >= 0, "all transaction streams are unsettled; recover outstanding keys");
        auto& s = streams_[p.stream];
        require(s.sequence != UINT64_MAX, "transaction sequence exhausted");
        std::ostringstream out; out << s.session << '_' << std::hex << ++s.sequence;
        return out.str();
    }
    void clear_pending() {
        auto& p = *pending();
        if (p.barrier) parity_barrier_.leave(p.stripe_ids, true);
        if (p.stream >= 0) streams_[p.stream].busy = false;
        pending().reset(); --pending_count_;
        operation_->entry->inplace = false; operation_->entry->unsettled = false;
    }
    void reclaim() {
        for (auto it = deferred_.begin(); it != deferred_.end();) {
            if (it->epoch.use_count() != 1) { ++it; continue; }
            while (it->released < it->fragments.size()) {
                const auto& f = it->fragments[it->released];
                trees_[it->group].release(f); ++it->released;
                reader_retained_bytes_ -= f.length; ++reclaimed_fragments_;
            }
            it = deferred_.erase(it);
        }
    }
    uint64_t logical_live_bytes_ = 0, fragment_count_ = 0, key_bytes_ = 0;
    uint64_t fragment_heap_slots_ = 0;
    uint64_t update_failures_ = 0;
    uint64_t next_stripe_ = 0, cursor_ = 0;
    uint64_t used_data_chunks_ = 0, used_stripes_ = 0; // Cumulative diagnostic usage, including load phase.
    uint64_t puts_ = 0, updates_ = 0, resizes_ = 0, deletes_ = 0, gets_ = 0, repaired_ = 0;
    uint64_t allocation_failures_ = 0;
    uint64_t reallocated_updates_ = 0, node_migrations_ = 0, coefficient_changes_ = 0;
    struct PlacementDecision {
        int group = -1;
        bool existing_space = false;
        uint64_t parity_bytes = 0;
        uint64_t update_number = 0;
        std::vector<uint64_t> load_bytes, data_bytes;
    } last_placement_;
    // Lock order: per-key writer -> per-key reader gate -> metadata -> queue.
    // Transaction/read network waits and stripe-fence waits release metadata.
    // RPC workers never call Store. Epoch pins defer RELEASE, without data GC.
    std::mutex mutex_;
    static mosaic::Parameters parameters() {
        mosaic::Parameters p{
            static_cast<size_t>(integer(env("ALIGNMENT_CHUNK", "4096"), 64, mosaic::MAX_ALIGNMENT_CHUNK)),
            static_cast<size_t>(integer(env("MOSAIC_FMAX", "4"), 1, 4096)),
            static_cast<size_t>(integer(env("MOSAIC_TAU", "8"), 0, 1048576))};
        p.validate();
        return p;
    }
    int group(int node, int coefficient) const { return node * k_ + coefficient; }
    static uint64_t now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    void record_parity(int start, uint64_t bytes) {
        if (!resize_phase()) return;
        for (int j = 0; j < m_; ++j) parity_load_.record(layout_.node(start, k_ + j), bytes);
    }
    void record_data(int node, uint64_t bytes) {
        if (resize_phase()) data_load_.record(node, bytes);
    }
    std::string key_id(const std::string& key) const {
        require(!key.empty() && key.size() <= MAX_KEY_BYTES, "key must contain 1..128 bytes");
        return key;
    }
    const Object& lookup(const std::string& key) {
        auto entry = objects_.find(key_id(key));
        auto v = entry ? entry->load() : nullptr;
        if (!v) throw Error("NOT_FOUND", "key not found");
        return v->object; // The caller holds this key's writer or reader lock.
    }
    std::string version(const Stripe& stripe) const {
        uint64_t low = stripe_seed_[1] + stripe.id;
        uint64_t high = stripe_seed_[0] + (low < stripe_seed_[1]);
        std::string value(32, '0');
        constexpr auto digits = "0123456789abcdef";
        for (int i = 0; i < 16; ++i) {
            value[15 - i] = digits[high & 15]; high >>= 4;
            value[31 - i] = digits[low & 15]; low >>= 4;
        }
        return value;
    }
    std::string name(const Stripe& stripe, int index) const { return version(stripe) + "_" + std::to_string(index); }
    std::future<Frames> submit(int node, Frames request, bool rmw = false, int timeout = 0) {
        request.insert(request.begin() + 1, std::to_string(node));
        request = {"MOSAIC", std::to_string(node), request[0], std::to_string(parameters_.fmax), pack_record(request)};
        return workers_.submit(node, std::move(request), timeout ? timeout : (rmw ? 3 * timeout_ + 1000 : timeout_));
    }
    Frames checked(std::future<Frames>& job, bool concurrent = false) {
        Frames reply;
        if (concurrent) { Unlocked unlocked; reply = job.get(); }
        else reply = job.get();
        if (reply.size() == 2 && reply[0] == "MOSAIC") {
            auto fields = unpack_record(reply[1]);
            require(fields.size() == 4, "invalid Mosaic reply");
            const auto node = integer(fields[2], 0, nodes_.size()-1);
            std::istringstream report(fields[3]);
            for (int c = 0; c < k_; ++c) trees_[group(node, c)].report(report);
            reply = {std::move(fields[0]), std::move(fields[1])};
        }
        if (reply.size() != 2) throw Error("UNAVAILABLE", "invalid node reply");
        if (reply[0] != "OK") throw Error(reply[0], reply[1]);
        return reply;
    }
    Frames rpc(int node, Frames request, bool rmw = false) {
        auto job = submit(node, std::move(request), rmw);
        return checked(job);
    }
    Frames concurrent_rpc(int node, Frames request, bool rmw = false) {
        auto job = submit(node, std::move(request), rmw);
        return checked(job, true);
    }
    struct RecoveryGuard {
        MosaicStore& store;
        std::vector<uint64_t> ids;
        RecoveryGuard(MosaicStore& s, std::vector<uint64_t> keys) : store(s), ids(std::move(keys)) {
            Unlocked unlocked; store.parity_barrier_.enter(ids, false, 3*store.timeout_ + 1000);
        }
        ~RecoveryGuard() { store.parity_barrier_.leave(ids, false); }
    };
    void cleanup() {
        if (materializing_) finish_materialization();
        while (!retired_holes_.empty()) {
            const auto [g, id] = retired_holes_.back();
            trees_[g].remove_empty_chunk(id);
            retired_holes_.pop_back();
        }
        size_t attempts = 0;
        for (auto it = retired_.begin(); it != retired_.end() && attempts++ < 4;) {
            try {
                for (int i = 0; i < k_ + m_; ++i)
                    rpc(layout_.node(it->second, i), {"DELETE", it->first + "_" + std::to_string(i)});
                it = retired_.erase(it);
            } catch (...) { ++it; }
        }
    }
    bool virtual_zero(const Stripe& stripe, int index) const {
        return index < k_ && !(stripe.materialized_mask & (uint32_t(1) << index));
    }
    void finish_materialization() {
        const auto [id, coefficient] = *materializing_;
        const auto& stripe = stripes_.at(id);
        const int node = layout_.node(stripe.start, coefficient), g = group(node, coefficient);
        // Zero-fill happens locally and creation is idempotent, including late
        // requests: an already materialized chunk is never overwritten.
        rpc(node, {"ZERO_CHUNK", name(stripe, coefficient), std::to_string(parameters_.alignment_chunk)});
        trees_[g].add_chunk(id, parameters_.alignment_chunk);
        stripes_.materialize(id, coefficient);
        ++materialized_data_chunks_;
        virgin_[g].pop_back();
        materializing_.reset();
    }
    void provision(int node, int coefficient) {
        auto& available = virgin_[group(node, coefficient)];
        if (!available.empty()) {
            materializing_ = {available.back(), coefficient};
            finish_materialization();
            return;
        }
        require(next_stripe_ < UINT64_MAX, "chunk ID space exhausted");
        auto id = next_stripe_++;
        // Materialize only the selected data chunk and M parity chunks.
        Stripe stripe{id, static_cast<uint16_t>(layout_.start_for(node, coefficient))};
        stripe.materialized_mask = uint32_t(1) << coefficient;
        const auto physical_version = version(stripe);
        retired_.emplace(physical_version, stripe.start);
        bool registering = false;
        retired_holes_.reserve(retired_holes_.size() + 1);
        try {
            stripes_.prepare(stripe);
            for (int c = 0; c < k_; ++c) if (c != coefficient) {
                auto& queue = virgin_[group(layout_.node(stripe.start, c), c)];
                queue.reserve(queue.size() + 1);
            }
            Replies batch;
            batch.jobs.reserve(1 + m_);
            for (int i = 0; i < k_ + m_; ++i) if (i == coefficient || i >= k_)
                batch.jobs.push_back(submit(layout_.node(stripe.start, i), {"ZERO_CHUNK", name(stripe, i), std::to_string(parameters_.alignment_chunk)}));
            for (auto& job : batch.jobs) checked(job);
            registering = true;
            trees_[group(node, coefficient)].add_chunk(id, parameters_.alignment_chunk);
        } catch (...) {
            // Include the operation whose reply may have been lost. Drain its
            // idempotent registration before removing the unpublished chunk.
            if (registering) retired_holes_.emplace_back(group(node, coefficient), id);
            stripes_.cancel();
            throw;
        }
        stripes_.commit();
        ++materialized_data_chunks_;
        for (int c = 0; c < k_; ++c) if (c != coefficient)
            virgin_[group(layout_.node(stripe.start, c), c)].push_back(id);
        retired_.erase(physical_version);
    }
    std::pair<int, mosaic::Plan> choose(size_t length, const Object* old, bool balance, Profile* trace,
                                         size_t fragment_budget = 0) {
        const size_t budget = fragment_budget ? fragment_budget : parameters_.fmax;
        std::vector<int> candidates;
        std::vector<uint64_t> load, data, cost;
        if (balance) {
            load.resize(nodes_.size()); data.resize(nodes_.size()); cost.resize(nodes_.size());
            std::vector<int> starts;
            for (size_t node = 0; node < nodes_.size(); ++node) {
                load[node] = parity_load_.bytes(node) + data_load_.bytes(node);
                data[node] = load[node];
                starts.push_back((cursor_ + node) % nodes_.size());
            }
            for (size_t start = 0; start < nodes_.size(); ++start)
                for (int j = 0; j < m_; ++j) cost[start] += load[layout_.node(start, k_ + j)];
            // First rank parity combinations. Only then rank the K data nodes
            // associated with each combination, independently of other groups.
            std::stable_sort(starts.begin(), starts.end(), [&](int a, int b) { return cost[a] < cost[b]; });
            for (int start : starts) {
                std::vector<int> members;
                for (int i = 0; i < k_; ++i) {
                    int coefficient = i; // Algorithm 2 breaks equal loads by coding position.
                    members.push_back(group(layout_.node(start, coefficient), coefficient));
                }
                std::stable_sort(members.begin(), members.end(), [&](int a, int b) {
                    return data[a / k_] < data[b / k_];
                });
                candidates.insert(candidates.end(), members.begin(), members.end());
            }
        } else if (old && old->node(k_) >= 0) {
            candidates.push_back(group(old->node(k_), old->coefficient(k_)));
        } else {
            // Initial keys rotate over nodes, without capacity/load accounting.
            // Reuse any coefficient class on the selected node before growing.
            int node = cursor_ % nodes_.size();
            for (int i = 0; i < k_; ++i)
                candidates.push_back(group(node, (cursor_ / nodes_.size() + i) % k_));
        }
        auto decision = [&](int g, bool existing) {
            if (balance) last_placement_ = {g, existing, cost[layout_.start_for(g / k_, g % k_)],
                                           object_updates_ + 1, std::move(load), std::move(data)};
        };
        // Keep old fragments reserved. For each parity group, choose its least
        // written feasible data node. Reuse existing holes before provisioning.
        for (int g : candidates)
            if (auto plan = trees_[g].allocate(length, budget, parameters_.tau)) {
                decision(g, true);
                return {g, std::move(*plan)};
            }
        // A retained APPEND passes a fragment budget: its prefix cannot move
        // to another node/coefficient. Only full relocations may use fallback.
        if (space_fallback_ == "family_load" && placement_ == "parity_bandwidth" &&
            resize_phase() && old && old->node(k_) >= 0 && !balance && !fragment_budget) {
            ++space_fallback_attempts_;
            int from = group(old->node(k_), old->coefficient(k_));
            int start = layout_.start_for(old->node(k_), old->coefficient(k_));
            uint64_t source_bytes = data_load_.bytes(old->node(k_));
            std::vector<int> alternatives;
            alternatives.reserve(k_ - 1);
            for (int c = 0; c < k_; ++c) {
                int node = layout_.node(start, c), g = group(node, c);
                if (g == from) continue;
                if (data_load_.bytes(node) > source_bytes) {
                    ++space_fallback_load_rejections_;
                    continue;
                }
                alternatives.push_back(g);
            }
            std::stable_sort(alternatives.begin(), alternatives.end(), [&](int a, int b) {
                auto left = data_load_.bytes(a / k_), right = data_load_.bytes(b / k_);
                return left != right ? left < right : trees_[a].free_bytes() > trees_[b].free_bytes();
            });
            for (int g : alternatives) {
                ++space_fallback_probes_;
                if (auto plan = trees_[g].allocate(length, budget, parameters_.tau)) {
                    ++space_fallback_selections_;
                    last_space_fallback_ = {from, g, source_bytes, data_load_.bytes(g / k_), object_updates_ + 1};
                    return {g, std::move(*plan)};
                }
            }
        }
        int selected = candidates.front();
        decision(selected, false);
        size_t needed = length / parameters_.alignment_chunk + (length % parameters_.alignment_chunk != 0);
        for (size_t i = 0; i < needed; ++i) {
            auto start = trace && trace->active() ? Profile::now() : 0;
            provision(selected / k_, selected % k_);
            if (trace && trace->active()) { trace->detail("provision_ns", Profile::now() - start); trace->detail("new_stripes", 1); }
            if (auto plan = trees_[selected].allocate(length, budget, parameters_.tau)) return {selected, std::move(*plan)};
        }
        ++allocation_failures_;
        throw Error("NO_SPACE", "no Mosaic allocation within Fmax");
    }
    void recover_pending(Profile* trace = nullptr) {
        if (!this->pending()) return;
        auto& pending = *this->pending();
        if (pending.aborting) {
            concurrent_rpc(pending.node, {"TX_ABORT", pending.batch_request[1]}, true);
            if (pending.reserved) {
                const auto& pieces = pending.allocation->pieces;
                while (pending.abort_released < pieces.size()) {
                    const auto& piece = pieces[pending.abort_released];
                    trees_[pending.group].release({piece.hole.chunk, piece.hole.offset, piece.take});
                    ++pending.abort_released;
                }
            }
            clear_pending();
            return;
        }
        if (pending.published) {
            if (!pending.batch_request.empty()) concurrent_rpc(pending.node, {"TX_COMMIT", pending.batch_request[1]}, true);
            for (const auto& r : pending.retirement) for (const auto& f : r.fragments) reader_retained_bytes_ += f.length;
            deferred_.splice(deferred_.end(), pending.retirement);
            clear_pending();
            return;
        }
        if (!pending.io_complete && !pending.actions.empty()) {
            if (pending.batch_request.empty()) {
                std::map<uint64_t, std::vector<const Action*>> chunks;
                for (const auto& action : pending.actions) chunks[action.fragment.chunk].push_back(&action);
                require(chunks.size() <= MAX_BATCH_CHUNK_BYTES / parameters_.alignment_chunk, "RMW batch exceeds 48 MiB of chunk data");
                Frames members;
                members.reserve(chunks.size());
                for (auto& entry : chunks) {
                    auto& actions = entry.second;
                    std::sort(actions.begin(), actions.end(), [](const Action* a, const Action* b) { return a->fragment.offset < b->fragment.offset; });
                    const auto& stripe = stripes_.at(entry.first);
                    int index = actions[0]->coefficient;
                    std::ostringstream header;
                    header << index << ' ' << k_ << ' ' << m_ << ' ' << parameters_.alignment_chunk << '\n';
                    for (int j = 0; j < m_; ++j)
                        header << layout_.node(stripe.start, k_ + j) << ' ' << name(stripe, k_ + j) << '\n';
                    std::string ranges;
                    size_t packed_bytes = 0;
                    for (const auto* action : actions) packed_bytes += action->bytes.size() + 28;
                    ranges.reserve(packed_bytes);
                    for (const auto* action : actions) {
                        append_record_field(ranges, std::to_string(action->fragment.offset));
                        append_record_field(ranges, action->bytes);
                    }
                    members.push_back(pack_record_views({name(stripe, index), header.str(), ranges}));
                }
                Frames request{"TX_RMW", next_transaction(), pack_record_views(members)};
                pending.batch_request = std::move(request);
                // Network payload is the updated fragments, not local chunk I/O.
                pending.parity_bytes = 0;
                for (const auto& action : pending.actions) pending.parity_bytes += action.bytes.size();
                pending.parity_start = stripes_.at(chunks.begin()->first).start;
            }
            if (pending.allocation && !pending.reserved) {
                require(trees_[pending.group].commit(*pending.allocation), "stale allocation plan");
                pending.reserved = true;
            }
            if (!pending.barrier) {
                { Unlocked unlocked; parity_barrier_.enter(pending.stripe_ids, true, 3*timeout_ + 1000); }
                pending.barrier = true;
            }
            if (trace) { trace->id(pending.batch_request[1]); trace->mark("pending_batch_pack"); }
            // Provisioning zero-fill does not count as object use. Track before
            // sending: a failed/unknown outcome may already have changed bytes.
            // Never clear on release/delete; reused free chunks still occupy space.
            for (const auto& action : pending.actions) {
                const auto changed = stripes_.note_write(action.fragment.chunk, action.coefficient);
                used_data_chunks_ += bool(changed & 1);
                used_stripes_ += bool(changed & 2);
            }
            ++active_writes_; max_active_writes_ = std::max(max_active_writes_, active_writes_);
            try { concurrent_rpc(pending.node, pending.batch_request, true); --active_writes_; }
            catch (...) {
                --active_writes_;
                auto failure = std::current_exception();
                ++update_failures_;
                pending.aborting = true;
                // A timeout is an unknown outcome. Keep the fence and the
                // reservation until every affected shard confirms rollback.
                recover_pending();
                std::rethrow_exception(failure);
            }
            record_parity(pending.parity_start, pending.parity_bytes);
            record_data(pending.node, pending.parity_bytes);
            pending.io_complete = true;
            if (trace) trace->mark("data_agent_roundtrip");
        }
        // No allocation or I/O during map publication. Released holes retain
        // their bytes and parity contribution; later RMW reads those actual old
        // bytes before overwriting them. Only never-written regions start at zero.
        auto previous = operation_->entry->load();
        if (previous) {
            logical_live_bytes_ -= previous->object.size;
            fragment_count_ -= previous->object.fragments().size();
            fragment_heap_slots_ -= previous->object.fragments().heap_capacity();
            key_bytes_ -= pending.key.size(); --live_keys_;
        }
        if (!pending.erase) {
            const auto& next = pending.publication->object;
            logical_live_bytes_ += next.size;
            fragment_count_ += next.fragments().size();
            fragment_heap_slots_ += next.fragments().heap_capacity();
            key_bytes_ += pending.key.size();
            ++live_keys_;
        }
        operation_->entry->publish(std::move(pending.publication));
        if (pending.erase) ++deletes_;
        else if (pending.update) ++updates_;
        else ++puts_;
        if (pending.resize) ++resizes_;
        if (pending.reallocated) ++reallocated_updates_;
        if (pending.node_migration) ++node_migrations_;
        if (pending.coefficient_change) ++coefficient_changes_;
        if (pending.object_update) ++object_updates_;
        if (pending.rebalance) ++rebalance_events_;
        if (pending.space_fallback) ++space_fallback_updates_;
        if (pending.append) {
            ++appends_;
            if (pending.append_retained) ++append_retained_;
            else ++append_fallbacks_;
        }
        ++cursor_;
        pending.published = true;
        recover_pending();
        if (trace) trace->mark("publish_metadata");
    }
    void submit_mutation(Pending next, Object object, Profile* trace) {
        next.node = object.node(k_);
        auto previous = operation_->entry->load();
        const bool shares_extents = previous && next.releases.empty();
        if (!next.erase) next.publication = objects_.version(Version{std::move(object),
            shares_extents ? previous->epoch : objects_.record<ReadEpoch>()});
        if (!next.releases.empty()) next.retirement.push_back({previous->epoch, next.release_group, next.releases, 0});
        std::set<uint64_t> batch_ids;
        for (const auto& action : next.actions) batch_ids.insert(action.fragment.chunk);
        next.stripe_ids.assign(batch_ids.begin(), batch_ids.end());
        require(batch_ids.size() <= MAX_BATCH_CHUNK_BYTES / parameters_.alignment_chunk, "RMW batch exceeds 48 MiB of chunk data");
        if (trace) { trace->detail("chunks", batch_ids.size()); trace->detail("fragments", next.actions.size()); trace->mark("stage_metadata"); }
        // RMW reads and validates chunks itself. Reserve once, without preflight reads.
        pending() = std::make_unique<Pending>(std::move(next));
        ++pending_count_; operation_->entry->unsettled = true;
        recover_pending(trace);
    }
    void mutate(const std::string& key, const std::string& value, bool update, bool erase = false,
                Profile* trace = nullptr, bool append = false) {
        auto id = key_id(key);
        require(value.size() <= MAX_VALUE, "value exceeds 16 MiB");
        auto found = operation_->entry->load();
        const Object* old = found ? &found->object : nullptr;
        if ((update || erase) && !old) throw Error("NOT_FOUND", "key not found");
        if (!parameters_.fits(value.size())) {
            ++allocation_failures_;
            throw Error("NO_SPACE", "value exceeds Fmax * alignment_chunk; fragments cannot cross chunks");
        }
        Pending next;
        next.key = id; next.group = -1; next.erase = erase; next.update = update;
        next.append = append;
        next.resize = old && old->size != value.size() && !erase;
        next.object_update = old && !erase && resize_phase();
        next.rebalance = next.object_update && placement_ == "parity_bandwidth" &&
            next.resize && object_updates_ % rebalance_every_ == rebalance_every_ - 1;
        Object object;
        object.size = value.size();
        bool reuse = old && old->size == value.size() && !erase;
        if (reuse) operation_->entry->inplace = true;
        if (reuse) next.rebalance = false;
        if (reuse) object = *old;
        else if (old) object.group = old->group;
        if (trace) trace->mark("validate_lookup");
        if (!reuse && !value.empty() && !erase) {
            auto selected = choose(value.size(), old, next.rebalance, trace);
            if (trace) trace->mark("allocate");
            next.group = selected.first;
            object.group = static_cast<uint16_t>(next.group);
            object.assign_fragments(selected.second);
            next.allocation = std::move(selected.second);
        } else if (old && old->node(k_) >= 0 && !reuse) {
            next.group = group(old->node(k_), old->coefficient(k_));
        }
        size_t position = 0;
        if (!erase) for (const auto& fragment : object.fragments()) {
            next.actions.push_back({fragment, object.coefficient(k_), value.substr(position, fragment.length)});
            position += fragment.length;
        }
        if (old && !reuse) {
            next.releases.reserve(old->fragments().size());
            next.releases.assign(old->fragments().begin(), old->fragments().end());
            if (old->node(k_) >= 0) next.release_group = group(old->node(k_), old->coefficient(k_));
        }
        next.reallocated = old && !reuse && !erase && !value.empty();
        next.node_migration = next.reallocated && old->node(k_) >= 0 && old->node(k_) != object.node(k_);
        next.coefficient_change = next.reallocated && old->coefficient(k_) >= 0 && old->coefficient(k_) != object.coefficient(k_);
        next.space_fallback = next.node_migration && !next.rebalance;
        submit_mutation(std::move(next), std::move(object), trace);
    }
    void append(const std::string& key, const std::string& suffix, Profile* trace) {
        const auto& old = lookup(key);
        require(suffix.size() <= MAX_VALUE - old.size, "appended value exceeds 16 MiB");
        const size_t length = old.size + suffix.size();
        if (!parameters_.fits(length)) {
            ++allocation_failures_;
            throw Error("NO_SPACE", "value exceeds Fmax * alignment_chunk; fragments cannot cross chunks");
        }
        const size_t budget = parameters_.fmax - old.fragments().size();
        const size_t needed = suffix.size() / parameters_.alignment_chunk +
                              (suffix.size() % parameters_.alignment_chunk != 0);
        if (!append_enabled_ || needed > budget) {
            // Only the fallback reads the prefix. It uses the existing full
            // relocation path, including its usual placement policy.
            auto value = old.size ? get(old, false) : std::string{};
            value += suffix;
            if (trace) trace->mark("append_fallback_read");
            mutate(key, value, true, false, trace, true);
            return;
        }
        Pending next;
        next.key = key; next.update = true; next.append = true; next.append_retained = true;
        next.resize = !suffix.empty(); next.object_update = resize_phase();
        // Retaining the prefix also retains its node/coefficient, even on a
        // rebalance boundary. Only full-relocation fallbacks can migrate it.
        Object object;
        object.group = old.group;
        if (suffix.empty()) object = old;
        object.size = length;
        if (trace) trace->mark("validate_lookup");
        if (!suffix.empty()) {
            auto selected = choose(suffix.size(), &old, false, trace, budget);
            if (trace) trace->mark("allocate");
            next.group = selected.first;
            object.group = static_cast<uint16_t>(next.group);
            mosaic::Plan combined{0, length, {}};
            combined.pieces.reserve(old.fragments().size() + selected.second.pieces.size());
            for (const auto& fragment : old.fragments()) combined.pieces.push_back({fragment, fragment.length});
            size_t position = 0;
            for (const auto& piece : selected.second.pieces) {
                Hole fragment{piece.hole.chunk, piece.hole.offset, piece.take};
                combined.pieces.push_back({fragment, piece.take});
                next.actions.push_back({fragment, object.coefficient(k_), suffix.substr(position, piece.take)});
                position += piece.take;
            }
            object.assign_fragments(combined);
            std::set<uint64_t> ids;
            for (const auto& fragment : object.fragments()) ids.insert(fragment.chunk);
            require(ids.size() <= MAX_BATCH_CHUNK_BYTES / parameters_.alignment_chunk,
                    "appended object exceeds 48 MiB of chunk data");
            // Commit/rollback only the added ranges; never release the prefix.
            next.allocation = std::move(selected.second);
        }
        submit_mutation(std::move(next), std::move(object), trace);
    }
    std::vector<std::string> read_stripe(const Stripe& stripe) {
        Replies batch;
        batch.jobs.reserve(k_ + m_);
        std::vector<int> indices;
        std::vector<std::string> blocks(k_ + m_);
        for (int i = 0; i < k_ + m_; ++i)
            if (virtual_zero(stripe, i)) blocks[i].assign(parameters_.alignment_chunk, '\0');
            else { indices.push_back(i); batch.jobs.push_back(submit(layout_.node(stripe.start, i), {"READ", name(stripe, i)})); }
        for (size_t j = 0; j < indices.size(); ++j) {
            try {
                auto value = checked(batch.jobs[j])[1];
                if (value.size() == parameters_.alignment_chunk) blocks[indices[j]] = std::move(value);
            } catch (const std::exception&) {} // Missing or invalid-length shards are erasures.
        }
        return blocks;
    }
    std::vector<std::string> recover(const std::vector<std::string>& blocks) {
        auto recovered = codec_.recover(blocks);
        return recovered;
    }
    Frames fragment_ranges(const std::vector<Hole>& fragments, int index) const {
        Frames ranges;
        for (const auto& f : fragments) {
            ranges.push_back(name(stripes_.at(f.chunk), index));
            ranges.push_back(std::to_string(parameters_.alignment_chunk));
            ranges.push_back(std::to_string(f.offset));
            ranges.push_back(std::to_string(f.length));
        }
        return ranges;
    }
    std::string recover_fragments(const std::vector<Hole>& fragments, int target) {
        require(!fragments.empty(), "empty fragment recovery");
        const auto start = stripes_.at(fragments.front().chunk).start;
        size_t length = 0;
        for (const auto& f : fragments) {
            require(stripes_.at(f.chunk).start == start, "fragment layout mismatch");
            length += f.length;
        }
        std::vector<std::string> blocks(k_ + m_);
        int good = 0, cursor = 0;
        while (good < k_ && cursor < k_ + m_) {
            Replies batch;
            std::vector<int> indices;
            while (cursor < k_ + m_ && static_cast<int>(indices.size()) < k_ - good) {
                int i = cursor++;
                if (i == target) continue;
                bool all_zero = true, any_zero = false;
                for (const auto& f : fragments) {
                    const bool zero = virtual_zero(stripes_.at(f.chunk), i);
                    all_zero &= zero; any_zero |= zero;
                }
                if (all_zero) { blocks[i].assign(length, '\0'); ++good; continue; }
                if (any_zero) {
                    // Mixed materialization across chunks: decode each range
                    // separately so absent virgin helpers remain known zeroes.
                    std::string value;
                    for (const auto& f : fragments) value += recover_fragments({f}, target);
                    return value;
                }
                indices.push_back(i);
                batch.jobs.push_back(submit(layout_.node(start, i),
                    {"READ_RANGES", pack_record(fragment_ranges(fragments, i))}));
            }
            for (size_t j = 0; j < indices.size(); ++j) {
                try {
                    auto value = checked(batch.jobs[j], true)[1];
                    if (value.size() == length) { blocks[indices[j]] = std::move(value); ++good; }
                } catch (const std::exception&) {} // Try spare helpers if needed.
            }
        }
        if (good < k_ && fragments.size() > 1) {
            // Different partial failures may leave K sources per fragment but
            // no common set of K helpers for the whole object.
            std::string value;
            value.reserve(length);
            for (const auto& f : fragments) value += recover_fragments({f}, target);
            return value;
        }
        return codec_.recover_target(blocks, target, length);
    }
    std::string get(const Object& object, bool count = true) {
        auto view = object.fragments();
        std::vector<Hole> fragments(view.begin(), view.end());
        std::string value;
        if (!fragments.empty()) {
            try {
                value = concurrent_rpc(object.node(k_), {"READ_RANGES",
                    pack_record(fragment_ranges(fragments, object.coefficient(k_)))})[1];
                require(value.size() == object.size, "value range length mismatch");
            } catch (const std::exception&) {
                std::set<uint64_t> unique;
                for (const auto& f : fragments) unique.insert(f.chunk);
                RecoveryGuard guard(*this, {unique.begin(), unique.end()});
                value = recover_fragments(fragments, object.coefficient(k_));
            }
        }
        require(value.size() == object.size, "value length mismatch");
        if (count) ++gets_;
        return value;
    }
    uint64_t repair_object(const Object& object, bool force = false) {
        auto view = object.fragments();
        if (view.empty()) return 0;
        std::vector<Hole> fragments(view.begin(), view.end()), missing;
        std::set<uint64_t> ids;
        for (const auto& f : fragments) ids.insert(f.chunk);
        RecoveryGuard guard(*this, {ids.begin(), ids.end()});
        std::string flags(fragments.size(), '0');
        if (!force) try {
            flags = rpc(object.node(k_), {"CHECK_RANGES",
                pack_record(fragment_ranges(fragments, object.coefficient(k_)))})[1];
            require(flags.size() == fragments.size() && flags.find_first_not_of("01") == std::string::npos,
                    "invalid range availability reply");
        } catch (const std::exception&) { flags.assign(fragments.size(), '0'); }
        for (size_t i = 0; i < fragments.size(); ++i) if (flags[i] == '0') missing.push_back(fragments[i]);
        if (missing.empty()) return 0;
        auto value = recover_fragments(missing, object.coefficient(k_));
        Frames ranges;
        std::set<uint64_t> chunks;
        size_t offset = 0;
        for (const auto& f : missing) {
            ranges.push_back(name(stripes_.at(f.chunk), object.coefficient(k_)));
            ranges.push_back(std::to_string(parameters_.alignment_chunk));
            ranges.push_back(std::to_string(f.offset));
            ranges.push_back(value.substr(offset, f.length));
            offset += f.length; chunks.insert(f.chunk);
        }
        // Restore original codeword bytes; parity must not receive a delta.
        rpc(object.node(k_), {"REPAIR_RANGES", pack_record(ranges)});
        repaired_ += chunks.size();
        record_data(object.node(k_), value.size());
        return chunks.size();
    }
    uint64_t repair(const std::set<uint64_t>& ids, int target = -1) {
        RecoveryGuard guard(*this, {ids.begin(), ids.end()});
        uint64_t count = 0;
        for (auto id : ids) {
            const auto& stripe = stripes_.at(id);
            auto blocks = read_stripe(stripe);
            std::vector<int> missing;
            for (int i = 0; i < k_ + m_; ++i)
                if (blocks[i].empty() && (target < 0 || layout_.node(stripe.start, i) == target)) missing.push_back(i);
            if (missing.empty()) continue;
            blocks = recover(blocks);
            Replies batch;
            batch.jobs.reserve(missing.size());
            for (int i : missing) batch.jobs.push_back(submit(layout_.node(stripe.start, i), {"WRITE", name(stripe, i), blocks[i]}));
            for (size_t j = 0; j < batch.jobs.size(); ++j) {
                checked(batch.jobs[j]); ++count; ++repaired_;
                if (missing[j] >= k_ && resize_phase())
                    parity_load_.record(layout_.node(stripe.start, missing[j]), parameters_.alignment_chunk);
                else if (missing[j] < k_) record_data(layout_.node(stripe.start, missing[j]), parameters_.alignment_chunk);
            }
        }
        return count;
    }
    std::string metadata(const Object& object) const {
        std::ostringstream out;
        out << "{\"size\":" << object.size << ",\"node\":" << object.node(k_)
            << ",\"coefficient_class\":" << object.coefficient(k_) << ",\"extents\":[";
        for (size_t f = 0; f < object.fragments().size(); ++f) {
            const auto& fragment = object.fragments()[f];
            const auto& stripe = stripes_.at(fragment.chunk);
            int index = object.coefficient(k_);
            out << (f ? "," : "") << "{\"storage\":\"ec\",\"stripe\":\"" << version(stripe) << "\",\"chunk\":" << fragment.chunk
                << ",\"node\":" << object.node(k_) << ",\"coefficient_class\":" << index << ",\"block_index\":" << index
                << ",\"offset\":" << index * parameters_.alignment_chunk + fragment.offset << ",\"block_offset\":" << fragment.offset
                << ",\"length\":" << fragment.length << ",\"block_bytes\":" << parameters_.alignment_chunk
                << ",\"data_blocks\":" << k_ << ",\"parity_blocks\":" << m_ << ",\"shards\":[";
            for (int i = 0; i < k_ + m_; ++i)
                out << (i ? "," : "") << "{\"index\":" << i << ",\"chunk\":" << fragment.chunk << ",\"node\":" << layout_.node(stripe.start, i)
                    << ",\"role\":\"" << (i < k_ ? "data" : "parity") << "\",\"block\":\"" << name(stripe, i) << "\"}";
            out << "]}";
        }
        out << "]}";
        return out.str();
    }
    std::string metadata_memory_report() const {
        namespace mm = metadata_memory;
        const auto catalog = objects_.snapshot();
        uint64_t objects = objects_.metadata_bytes(), stripes = stripes_.metadata_bytes() + mm::vector(virgin_);
        size_t virgin_count = 0;
        for (const auto& queue : virgin_) { stripes += mm::vector(queue); virgin_count += queue.size(); }
        uint64_t holes = mm::vector(trees_), retired = mm::nodes(retired_);
        for (const auto& [key, version] : catalog) {
            (void)key; objects += mm::allocation(version->object.fragments().heap_data());
        }
        for (const auto& tree : trees_) holes += tree.metadata_bytes();
        for (const auto& [version, node] : retired_) { (void)node; retired += mm::string(version); }
        uint64_t useful_objects = 0, key_bytes = 0, useful_holes = 0, useful_retired = 0;
        for (const auto& [key, version] : catalog) {
            const auto& object = version->object;
            key_bytes += key.size();
            const auto fragments = object.fragments();
            // size:u32, node/coefficient group:u16, count/format flags:u16.
            // Per-object descriptor width handles mixed small/wide/heap arrays.
            useful_objects += key.size() + 4 + 2 + 2 + fragments.size() * fragments.record_bytes();
        }
        uint64_t rb_holes = 0;
        for (const auto& tree : trees_) {
            useful_holes += tree.useful_metadata_bytes();
            rb_holes += tree.rb_tree_metadata_bytes();
        }
        for (const auto& [version, node] : retired_) { (void)node; useful_retired += version.size() + 4; }
        auto report = mm::with_rb_tree(mm::with_useful(mm::report({{"objects", objects}, {"stripes", stripes}, {"free_space", holes},
                           {"retirement", retired}, {"gc_candidates", 0}, {"space_counters", 0},
                            {"placement_counters", parity_load_.metadata_bytes() + data_load_.metadata_bytes()}}, live_keys_),
            {{"objects", useful_objects}, {"stripes", stripes_.size() * (8 + 2 + 4) + virgin_count * 8},
             {"free_space", useful_holes}, {"retirement", useful_retired}, {"gc_candidates", 0}, {"space_counters", 0},
             {"placement_counters", parity_load_.useful_metadata_bytes() + data_load_.useful_metadata_bytes()}}, live_keys_, key_bytes),
            {{"objects", 0}, {"stripes", 0}, {"buffers", 0},
             {"free_space", rb_holes}, {"retirement", mm::rb_tree_fields(retired_)}, {"gc_candidates", 0}});
        report.pop_back();
        return report + ",\"placement\":{\"free_space_owner\":\"memory_nodes\",\"proxy_capacity_cache_bytes\":"
            + std::to_string(mm::vector(trees_)) + ",\"node_hole_index_bytes\":"
            + std::to_string(holes - mm::vector(trees_)) + "}}";
    }
    std::string stats() {
        const auto live = logical_live_bytes_, fragments = fragment_count_;
        size_t free = 0, holes = 0;
        for (const auto& tree : trees_) { free += tree.free_bytes(); holes += tree.hole_count(); }
        std::ostringstream out;
        out << "{\"storage_mode\":\"memory\",\"storage_backend\":\"" << storage_backend() << "\",\"update_strategy\":\"mosaic\",\"coding_unit\":\"alignment_chunk\""
            << ",\"k\":" << k_ << ",\"m\":" << m_ << ",\"block_size\":" << parameters_.alignment_chunk
            << ",\"alignment_chunk\":" << parameters_.alignment_chunk << ",\"fmax\":" << parameters_.fmax << ",\"tau\":" << parameters_.tau
            << ",\"keys\":" << live_keys_ << ",\"stripes\":" << stripes_.size() << ",\"fragments\":" << fragments
            << ",\"logical_live_bytes\":" << live << ",\"free_data_bytes\":" << free << ",\"holes\":" << holes
            << ",\"reader_retained_data_bytes\":" << reader_retained_bytes_ << ",\"retired_fragment_maps\":" << deferred_.size()
            << ",\"reclaimed_fragments\":" << reclaimed_fragments_ << ",\"active_readers\":" << active_reads_
            << ",\"active_updates\":" << active_writes_ << ",\"max_concurrent_updates\":" << max_active_writes_
            << ",\"max_concurrent_readers\":" << max_active_reads_ << ",\"pending_keys\":" << pending_count_
            << ",\"object_index\":\"sharded_concurrent_hash\""
            << ",\"reserved_data_bytes\":" << materialized_data_chunks_ * parameters_.alignment_chunk
            << ",\"active_parity_bytes\":" << stripes_.size() * m_ * parameters_.alignment_chunk
            << ",\"virtual_zero_data_chunks\":" << stripes_.size() * k_ - materialized_data_chunks_
            << ",\"never_written_data_chunks\":" << materialized_data_chunks_ - used_data_chunks_
            << ",\"never_written_parity_chunks\":" << (stripes_.size() - used_stripes_) * m_
            << ",\"never_written_chunk_bytes\":" << (materialized_data_chunks_ + stripes_.size() * m_ - used_data_chunks_ - used_stripes_ * m_) * parameters_.alignment_chunk
            << ",\"space_accounted_data_bytes\":" << materialized_data_chunks_ * parameters_.alignment_chunk
            << ",\"space_accounted_parity_bytes\":" << stripes_.size() * m_ * parameters_.alignment_chunk
            << ",\"space_accounted_payload_bytes\":" << (materialized_data_chunks_ + stripes_.size() * m_) * parameters_.alignment_chunk
            << ",\"space_accounting_scope\":\"materialized_chunks\""
            << ",\"chunk_usage_tracking\":\"first_fragment_rmw_attempt_since_load\""
            << ",\"pending_update\":" << (pending_count_ || materializing_ ? "true" : "false") << ",\"retired_stripes\":" << retired_.size()
            << ",\"update_failures\":" << update_failures_
            << ",\"append_enabled\":" << (append_enabled_ ? "true" : "false")
            << ",\"appends\":" << appends_ << ",\"append_retained\":" << append_retained_
            << ",\"append_fallbacks\":" << append_fallbacks_
            << ",\"metadata\":{\"key_storage\":\"raw\",\"key_bytes\":" << key_bytes_
            << ",\"fragment_descriptor_bytes\":" << sizeof(Hole)
            << ",\"compact_fragment_bytes\":8,\"object_record_bytes\":" << sizeof(Object)
            << ",\"fragment_heap_slots\":" << fragment_heap_slots_
            << ",\"hole_records\":" << holes
            << ",\"hole_indices_per_record\":2,\"allocated_index_entries\":0,\"stats_accounting\":\"incremental\"}"
            << ",\"allocation_failures\":" << allocation_failures_ << ",\"puts\":" << puts_ << ",\"gets\":" << gets_
            << ",\"updates\":" << updates_ << ",\"resize_updates\":" << resizes_ << ",\"deletes\":" << deletes_
            << ",\"repaired_shards\":" << repaired_
            << ",\"placement_policy\":\"" << placement_ << "\",\"load_accounting\":\"cumulative\""
            << ",\"space_fallback_policy\":\"" << space_fallback_ << '"'
            << ",\"space_fallback_attempts\":" << space_fallback_attempts_
            << ",\"space_fallback_probes\":" << space_fallback_probes_
            << ",\"space_fallback_load_rejections\":" << space_fallback_load_rejections_
            << ",\"space_fallback_selections\":" << space_fallback_selections_
            << ",\"space_fallback_updates\":" << space_fallback_updates_
            << ",\"rebalance_every_updates\":" << rebalance_every_ << ",\"committed_object_updates\":" << object_updates_
            << ",\"rebalance_events\":" << rebalance_events_
            << ",\"reallocated_updates\":" << reallocated_updates_ << ",\"node_migrations\":" << node_migrations_
            << ",\"coefficient_changes\":" << coefficient_changes_
            << ",\"initial_placement\":\"round_robin\",\"workload_phase\":\"" << workload_phase()
            << "\",\"parity_load\":[";
        auto now = now_ms();
        for (size_t node = 0; node < nodes_.size(); ++node)
            out << (node ? "," : "") << "{\"node\":" << node << ",\"cumulative_bytes\":" << parity_load_.bytes(node)
                << ",\"bytes_per_second\":" << parity_load_.bytes_per_second(node, now) << '}';
        out << "],\"data_load\":[";
        for (size_t node = 0; node < nodes_.size(); ++node)
            out << (node ? "," : "") << "{\"node\":" << node << ",\"cumulative_bytes\":" << data_load_.bytes(node)
                << ",\"bytes_per_second\":" << data_load_.bytes_per_second(node, now) << '}';
        out << "],\"load_elapsed_ms\":" << (resize_phase() ? now - load_started_ms_ : 0) << ",\"last_placement\":";
        if (last_placement_.group < 0) out << "null";
        else {
            int g = last_placement_.group, start = layout_.start_for(g / k_, g % k_);
            out << "{\"node\":" << g / k_ << ",\"coefficient_class\":" << g % k_
                << ",\"existing_space\":" << (last_placement_.existing_space ? "true" : "false")
                << ",\"parity_cumulative_bytes\":" << last_placement_.parity_bytes
                << ",\"update_number\":" << last_placement_.update_number << ",\"parity_nodes\":[";
            for (int j = 0; j < m_; ++j) out << (j ? "," : "") << layout_.node(start, k_ + j);
            out << "],\"load_bytes_at_selection\":[";
            for (size_t i = 0; i < last_placement_.load_bytes.size(); ++i)
                out << (i ? "," : "") << last_placement_.load_bytes[i];
            out << "],\"data_bytes_at_selection\":[";
            for (size_t i = 0; i < last_placement_.data_bytes.size(); ++i)
                out << (i ? "," : "") << last_placement_.data_bytes[i];
            out << "]}";
        }
        out << ",\"last_space_fallback\":";
        if (last_space_fallback_.from < 0) out << "null";
        else {
            const auto& d = last_space_fallback_;
            out << "{\"source_node\":" << d.from / k_ << ",\"source_coefficient\":" << d.from % k_
                << ",\"target_node\":" << d.to / k_ << ",\"target_coefficient\":" << d.to % k_
                << ",\"source_cumulative_bytes\":" << d.source_bytes
                << ",\"target_cumulative_bytes\":" << d.target_bytes
                << ",\"update_number\":" << d.update_number
                << ",\"parity_start\":" << layout_.start_for(d.to / k_, d.to % k_) << '}';
        }
        out << ",\"node_requests\":" << workers_.stats() << ",\"nodes\":[";
        Replies batch;
        batch.jobs.reserve(nodes_.size());
        for (size_t node = 0; node < nodes_.size(); ++node) batch.jobs.push_back(submit(node, {"STATS"}, false, stats_timeout_));
        bool complete = true;
        for (size_t node = 0; node < nodes_.size(); ++node) {
            std::string info, error;
            try { info = checked(batch.jobs[node])[1]; }
            catch (const std::exception& e) { error = e.what(); }
            catch (...) { error = "unknown node STATS exception"; }
            complete &= !info.empty();
            out << (node ? "," : "") << "{\"id\":" << node << ",\"healthy\":" << (info.empty() ? "false" : "true")
                << ",\"parity\":" << (info.empty() ? "null" : info)
                << ",\"stats_timeout_ms\":" << stats_timeout_
                << ",\"error\":" << (info.empty() ? json_string(error.empty() ? "empty node STATS reply" : error) : "null") << '}';
        }
        out << "],\"stats_complete\":" << (complete ? "true" : "false") << '}';
        return out.str();
    }
public:
    MosaicStore() : k_(integer(env("EC_K", "6"), 1, 31)), m_(integer(env("EC_M", "3"), 1, 31)),
        timeout_(integer(env("NODE_TIMEOUT_MS", "1000"), 10, 60000)), parameters_(parameters()),
        codec_(k_, m_, parameters_.alignment_chunk), nodes_(load_nodes(env("NODES_FILE", "/app/nodes.conf"))),
        layout_(k_, m_, nodes_.size()), workers_(context_, nodes_, 4),
        parity_load_(nodes_.size(), load_started_ms_), data_load_(nodes_.size(), load_started_ms_),
        trees_(nodes_.size() * k_), virgin_(nodes_.size() * k_) {
        require(placement_ == "fixed" || placement_ == "parity_bandwidth", "invalid Mosaic placement policy");
        require(storage_backend() == "native", "Mosaic transactional updates require STORAGE_BACKEND=native");
        require(space_fallback_ == "none" || space_fallback_ == "family_load", "invalid Mosaic space fallback policy");
        const auto owner = uuid();
        for (size_t node = 0; node < nodes_.size(); ++node)
            for (int c = 0; c < k_; ++c)
                trees_[group(node, c)].bind(owner, c, parameters_.fmax,
                    [this, node](Frames request) { return rpc(node, std::move(request)); });
    }
    ~MosaicStore() { workers_.stop(); } // Join while all Store members/context still exist.
    Frames handle_locked(const Frames& request) {
        require(!request.empty(), "missing operation");
        const auto& op = request[0];
        if (op == "PING") { require(request.size() == 1, "PING frame count"); return {"OK", "ec-kv/mosaic"}; }
        if (op == "METADATA_MEMORY") { require(request.size() == 1, "METADATA_MEMORY frame count"); return {"OK", metadata_memory_report()}; }
        if (op == "STATS") { require(request.size() == 1, "STATS frame count"); return {"OK", stats()}; }
        if (op == "PHASE") {
            require(request.size() == 2 && request[1] == "RESIZE", "PHASE requires RESIZE");
            require(!pending_count_, "cannot change phase during an unfinished mutation");
            Replies batch;
            batch.jobs.reserve(nodes_.size());
            for (size_t node = 0; node < nodes_.size(); ++node)
                batch.jobs.push_back(submit(node, {"PHASE", "RESIZE"}));
            for (auto& job : batch.jobs) checked(job);
            if (!resize_phase()) {
                load_started_ms_ = now_ms();
                parity_load_.reset(load_started_ms_); data_load_.reset(load_started_ms_);
                begin_resize_phase();
            }
            return {"OK", "resize"};
        }
        if (op == "PROFILE") {
            require(request.size() == 2 && (request[1] == "READ" || request[1] == "RESET"), "PROFILE READ/RESET");
            return {"OK", Profile::dump(request[1] == "RESET")};
        }
        Profile trace("proxy", op == "PUT" || op == "UPDATE" || op == "DELETE" || op == "APPEND");
        reclaim();
        if (operation_->entry) recover_pending();
        cleanup();
        trace.mark("pending_cleanup");
        if (op == "APPEND") {
            require(request.size() == 3, "APPEND requires key and suffix");
            append(request[1], request[2], &trace);
            return {"OK", ""};
        }
        if (op == "PUT" || op == "UPDATE") {
            require(request.size() == 3, "PUT/UPDATE requires key and value");
            mutate(request[1], request[2], op == "UPDATE", false, &trace);
            return {"OK", ""};
        }
        if (op == "REPAIR_BENCH") {
            require(request.size() == 3, "REPAIR_BENCH key target_node");
            const auto& object = lookup(request[1]);
            require(integer(request[2], 0, nodes_.size() - 1) == object.node(k_),
                    "object fragment repair targets its data node");
            return {"OK", std::to_string(repair_object(object, true))};
        }
        require(request.size() == 2, "operation requires one argument");
        if (op == "GET") return {"OK", get(lookup(request[1]))};
        if (op == "META") return {"OK", metadata(lookup(request[1]))};
        if (op == "DELETE") { mutate(request[1], "", false, true, &trace); return {"OK", ""}; }
        if (op == "REPAIR") {
            return {"OK", std::to_string(repair_object(lookup(request[1])))};
        }
        if (op == "REPAIR_NODE") {
            int target = integer(request[1], 0, nodes_.size() - 1);
            std::set<uint64_t> ids;
            for (size_t i = 0; i < stripes_.size(); ++i) ids.insert(stripes_[i].id);
            return {"OK", std::to_string(repair(ids, target))};
        }
        throw Error("INVALID", "unknown Mosaic operation");
    }
    Frames handle(const Frames& request) {
        require(!request.empty(), "missing operation");
        const auto& op = request[0];
        const bool keyed = op == "GET" || op == "PUT" || op == "UPDATE" || op == "DELETE" ||
            op == "APPEND" || op == "META" || op == "REPAIR" || op == "REPAIR_BENCH";
        if (!keyed) {
            std::unique_lock<std::mutex> lock(mutex_);
            Operation operation{{}, &lock}; OperationScope scope(operation);
            reclaim();
            return handle_locked(request);
        }
        require(request.size() >= 2, "missing key");
        auto key = key_id(request[1]);
        auto entry = objects_.find(key, op == "PUT");
        if (!entry) throw Error("NOT_FOUND", "key not found");
        if (op == "GET") {
            require(request.size() == 2, "GET frame count");
            // A completed-but-uncertain request can be settled by the next GET.
            // An active relocation owns writer, so readers keep using the old map.
            if (entry->unsettled) {
                std::unique_lock<std::mutex> writer(entry->writer, std::try_to_lock);
                if (writer.owns_lock()) {
                    std::unique_lock<std::shared_mutex> exclusive(entry->readers);
                    std::unique_lock<std::mutex> lock(mutex_);
                    Operation operation{entry, &lock}; OperationScope scope(operation);
                    recover_pending(); reclaim();
                }
            }
            std::shared_lock<std::shared_mutex> reader(entry->readers);
            auto version = entry->load();
            if (!version) throw Error("NOT_FOUND", "key not found");
            require(!entry->inplace, "key has an unsettled in-place update");
            std::unique_lock<std::mutex> lock(mutex_);
            Operation operation{entry, &lock}; OperationScope scope(operation);
            ++active_reads_; max_active_reads_ = std::max(max_active_reads_, active_reads_);
            try {
                auto value = get(version->object);
                --active_reads_; version.reset(); reader.unlock();
                reclaim(); lock.unlock(); objects_.prune(key, entry);
                return {"OK", std::move(value)};
            } catch (...) {
                if (reader.owns_lock()) { --active_reads_; version.reset(); reader.unlock(); }
                if (lock.owns_lock()) { try { reclaim(); } catch (...) {} }
                throw;
            }
        }
        // Writers of one key serialize independently of all other keys.
        std::unique_lock<std::mutex> writer(entry->writer);
        auto current = entry->load();
        const bool inplace = entry->inplace || op == "REPAIR" || op == "REPAIR_BENCH" ||
            ((op == "PUT" || op == "UPDATE") && request.size() == 3 && current && current->object.size == request[2].size()) ||
            (op == "APPEND" && request.size() == 3 && request[2].empty());
        current.reset();
        std::unique_lock<std::shared_mutex> exclusive(entry->readers, std::defer_lock);
        if (inplace) exclusive.lock();
        std::unique_lock<std::mutex> lock(mutex_);
        Operation operation{entry, &lock}; OperationScope scope(operation);
        try {
            auto reply = handle_locked(request);
            reclaim(); lock.unlock();
            if (exclusive.owns_lock()) exclusive.unlock();
            writer.unlock(); objects_.prune(key, entry);
            return reply;
        } catch (...) {
            if (lock.owns_lock() && !entry->pending) entry->inplace = false;
            if (lock.owns_lock()) lock.unlock();
            if (exclusive.owns_lock()) exclusive.unlock();
            if (writer.owns_lock()) writer.unlock();
            objects_.prune(key, entry);
            throw;
        }
    }
};
} // namespace ec
