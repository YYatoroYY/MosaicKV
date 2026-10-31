#include "codec.h"
#include "parity_workers.h"
#include "request_workers.h"
#include "gc_planner.h"
#include "gc_index.h"
#include "compact_baseline.h"
#include "mosaic_store.h"
#include "concurrent_server.h"
#include <future>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <optional>
using namespace ec;

template<bool WithGc> struct StripeRecord {
    StripeId version;
    uint16_t start = 0;
    uint16_t data_blocks = 0;
    int block_bytes = 0;
    std::vector<uint32_t> lengths;
    // Non-owning pointers to stable catalog key nodes. Only dereference under
    // Store::mutex_; GC snapshots copy key strings before releasing the lock.
    std::vector<const std::string*> owners;
    uint32_t live_mask = 0;
    StripeEpoch<WithGc> epoch;
};
using Extent = CompactExtent;
struct Object {
    InlineExtents extents;
    Object() = default;
    Object(InlineExtents positions) : extents(std::move(positions)) {}
    size_t size() const { size_t bytes = 0; for (const auto& e : extents) bytes += e.length; return bytes; }
};
static_assert(sizeof(Object) == 16);
struct BufferedBlock {
    StripeId version; uint32_t used = 0, bytes = 0;
    std::vector<int> replicas;
    std::vector<const std::string*> owners;
};
struct RetiredStripe {
    int start = 0, count = 0, bytes = 0;
    std::vector<uint32_t> stored_lengths = {};
    size_t stored(int i) const { return stored_lengths.empty() ? bytes : stored_lengths.at(i); }
};
template<bool WithGc> struct CatalogRecord {
    using Stripe = StripeRecord<WithGc>;
    bool object_layout = false;
    std::string tail = "-";
    size_t used = 0;
    std::map<StripeId, Stripe> stripes;
    std::map<StripeId, BufferedBlock> buffers;
    std::vector<StripeId> buffer_order;
    std::map<StripeId, std::vector<int>> retired_buffers;
    std::map<StripeId, RetiredStripe> retired_stripes;
    std::map<std::string, Object> keys; // Raw binary keys; snapshots retain their hex wire format.
};
template<bool WithGc> class Store {
    using Stripe = StripeRecord<WithGc>;
    using Catalog = CatalogRecord<WithGc>;
    int k_, m_, bytes_, timeout_;
    int stats_timeout_ = integer(env("STATS_TIMEOUT_MS", "10000"), 10, 60000);
    Codec codec_;
    std::map<std::pair<int, int>, std::unique_ptr<Codec>> smaller_codecs_;
    std::string coding_unit_ = env("CODING_UNIT", "object");
    std::string strategy_ = env("UPDATE_STRATEGY", "mosaic");
    bool replica_cleanup_ = integer(env("TEMP_REPLICA_CLEANUP", "1"), 0, 1);
    uint64_t buffer_delete_requests_ = 0;
    uint64_t buffer_delete_items_ = 0, buffer_cleanup_rounds_ = 0;
    Context context_;
    std::vector<std::string> nodes_;
    // Counters outlive every sender job (workers are destroyed first).
    RpcTrafficGroup regroup_traffic_, gc_traffic_;
    ParityWorkers buffer_writers_;
    std::unique_ptr<RequestWorkers> node_requests_;
    Catalog catalog_;
    std::map<StripeId, std::string> local_buffers_;
    StripeAccounting gc_index_;
    GcSpacePeaks gc_space_peaks_;
    GcAmplificationPeak gc_amplification_peak_;
    uint64_t logical_live_bytes_ = 0, gc_candidate_owner_visits_ = 0;
    size_t gc_snapshot_max_ = 0;
    bool rmw_active_ = false;
    uint64_t foreground_owner_visits_ = 0, direct_rmw_updates_ = 0;
    std::map<StripeId, RetiredStripe> gc_intent_;
    // Foreground lock order: mutex_ -> worker queue mutex. Worker jobs own their
    // requests and never call Store or acquire mutex_. GC snapshots are values;
    // bulk GC work and destructor join happen without mutex_.
    std::mutex mutex_;
    std::condition_variable gc_cv_;
    std::thread gc_thread_;
    int gc_interval_ = integer(env("GC_INTERVAL_MS", "1000"), 10, 3600000);
    uint64_t gc_every_updates_ = integer(env("GC_EVERY_UPDATES", strategy_ == "fastgc" ? "5000" : "0"), 0, 1000000000);
    uint64_t gc_update_triggers_ = 0, gc_update_pending_ = 0, gc_update_completed_ = 0;
    uint64_t gc_update_coalesced_ = 0, gc_update_started_ = 0;
    uint64_t gc_counted_updates_ = 0;
    uint64_t gc_drain_batches_ = 0;
    size_t gc_candidate_count_ = 0;
    int gc_start_percent_ = integer(env("GC_START_PERCENT", "20"), 1, 100);
    int gc_stop_percent_ = integer(env("GC_STOP_PERCENT", "10"), 0, 99);
    int gc_space_start_ = integer(env("GC_SPACE_START_PERCENT", "80"), 1, 100);
    int gc_space_stop_ = integer(env("GC_SPACE_STOP_PERCENT", "70"), 0, 99);
    int gc_batch_ = integer(env("GC_BATCH_STRIPES", "32"), 2, 128);
    uint64_t gc_capacity_ = gc_capacity();
    bool gc_stop_ = false, gc_wake_ = false, gc_forced_ = false, gc_active_ = false;
    bool gc_busy_ = false, gc_suspended_ = false;
    bool gc_paused_ = integer(env("GC_PAUSED", "0"), 0, 1);
    std::string gc_phase_ = "idle";
    uint64_t gc_cycles_ = 0, gc_commits_ = 0, gc_conflicts_ = 0, gc_failures_ = 0, gc_no_plan_ = 0;
    uint64_t gc_sources_ = 0, gc_outputs_ = 0, gc_net_bytes_ = 0, gc_reclaimed_bytes_ = 0;
    uint64_t gc_cloned_ = 0, gc_read_bytes_ = 0, gc_write_bytes_ = 0, gc_cleanup_failures_ = 0;
    StripeId gc_cursor_;
    uint64_t puts_ = 0, gets_ = 0, updates_ = 0, deletes_ = 0, repairs_ = 0;
    void record_write(bool update) {
        if (!update) { ++puts_; return; }
        ++updates_;
        if (strategy_ == "fastgc" && resize_phase()) ++gc_counted_updates_;
        if (strategy_ == "fastgc" && resize_phase() && gc_every_updates_ && gc_counted_updates_ % gc_every_updates_ == 0) {
            ++gc_update_triggers_;
            // One background worker and at most one follow-up pass. New
            // triggers must not be lost while a pass is finishing: both this
            // flag and its consumption in gc_loop are protected by mutex_.
            if (gc_update_pending_) ++gc_update_coalesced_;
            else gc_update_pending_ = 1;
            gc_cv_.notify_one();
        }
    }
    uint64_t update_failures_ = 0;
    uint64_t resizes_ = 0, peeled_ = 0, reencoded_ = 0, regrouped_ = 0;
    uint64_t inplace_reencodes_ = 0, inplace_buffer_updates_ = 0, inplace_delta_updates_ = 0;
    size_t foreground_keys_max_ = 0, foreground_stripes_max_ = 0;
    int width(const Stripe& stripe) const { return stripe.data_blocks + m_; }
    void observe_gc_space() noexcept {
        gc_space_peaks_.observe(gc_index_.total, gc_index_.invalid, gc_index_.logical, logical_live_bytes_);
        uint64_t replicas = 0;
        // At most K-1 unsealed objects in the normal object layout. Do not scan
        // the stripe/key catalog or issue monitoring RPCs in the measured path.
        for (const auto& entry : catalog_.buffers)
            replicas += entry.second.bytes * entry.second.replicas.size();
        gc_amplification_peak_.observe(gc_index_.total, gc_index_.parity, replicas, logical_live_bytes_);
    }
    Codec& codec_for(const Stripe& stripe) {
        if (stripe.data_blocks == k_ && stripe.block_bytes == bytes_) return codec_;
        auto& codec = smaller_codecs_[{stripe.data_blocks, stripe.block_bytes}];
        if (!codec) codec = std::make_unique<Codec>(stripe.data_blocks, m_, stripe.block_bytes);
        return *codec;
    }
    size_t capacity() const { return static_cast<size_t>(k_) * bytes_; }
    static bool valid_id(const std::string& s) {
        return s.size() == 32 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
    }
    std::string key_id(const std::string& key) const {
        require(!key.empty() && key.size() <= MAX_KEY_BYTES, "key must contain 1..128 bytes");
        return key;
    }
    const Object& lookup(const std::string& key) const {
        auto it = catalog_.keys.find(key_id(key));
        if (it == catalog_.keys.end()) throw Error("NOT_FOUND", "key not found");
        return it->second;
    }
    std::string check_owner_references() const {
        // Explicit diagnostic only. Check addresses before dereferencing them,
        // so a stale borrowed key is reported rather than read after free.
        std::map<const std::string*, const Object*> live;
        std::set<const std::string*> seen;
        for (const auto& [key, object] : catalog_.keys) live.emplace(&key, &object);
        auto check = [&](const auto& groups) {
            for (const auto& [id, record] : groups) for (const auto* owner : record.owners) {
                auto found = live.find(owner);
                require(found != live.end(), "stale owner reference");
                require(found->second->extents.at(0).stripe == id, "owner references wrong stripe");
                require(seen.insert(owner).second, "duplicate owner reference");
            }
        };
        check(catalog_.stripes); check(catalog_.buffers);
        const auto expected = catalog_.object_layout ? (strategy_ == "inplace" ? catalog_.buffers.size() : live.size()) : 0;
        require(seen.size() == expected, "missing owner reference");
        if (catalog_.object_layout) for (const auto& [key, object] : catalog_.keys) {
            (void)key; const auto& e = object.extents.at(0);
            auto stripe = catalog_.stripes.find(e.stripe);
            if (stripe != catalog_.stripes.end()) {
                require(e.offset < stripe->second.lengths.size() && e.length <= stripe->second.lengths[e.offset], "invalid object slot");
                if (strategy_ == "inplace") require(stripe->second.owners.empty(), "unexpected InPlace member index");
            }
        }
        return "{\"keys\":" + std::to_string(live.size()) + ",\"owner_refs\":" + std::to_string(seen.size()) + "}";
    }
    std::string metadata_memory_report() const {
        namespace mm = ec::metadata_memory;
        uint64_t objects = mm::nodes(catalog_.keys), stripes = mm::nodes(catalog_.stripes);
        for (const auto& [key, object] : catalog_.keys)
            objects += mm::string(key) + mm::allocation(object.extents.heap_data()) + mm::allocation(object.extents.heap_control_data());
        for (const auto& [id, stripe] : catalog_.stripes) stripes += mm::vector(stripe.lengths);
        uint64_t owners = 0;
        for (const auto& [id, stripe] : catalog_.stripes) owners += mm::vector(stripe.owners);
        for (const auto& [id, buffer] : catalog_.buffers) owners += mm::vector(buffer.owners);
        uint64_t gc = mm::nodes(gc_index_.dirty) + mm::nodes(gc_index_.empty);
        uint64_t buffers = mm::nodes(catalog_.buffers) + mm::vector(catalog_.buffer_order) + mm::nodes(local_buffers_);
        for (const auto& [id, buffer] : catalog_.buffers) buffers += mm::vector(buffer.replicas);
        uint64_t retired = mm::nodes(catalog_.retired_buffers) + mm::nodes(catalog_.retired_stripes) + mm::nodes(gc_intent_);
        for (const auto& [id, replicas] : catalog_.retired_buffers) retired += mm::vector(replicas);
        for (const auto& [id, stripe] : catalog_.retired_stripes) retired += mm::vector(stripe.stored_lengths);
        for (const auto& [id, stripe] : gc_intent_) retired += mm::vector(stripe.stored_lengths);
        uint64_t useful_objects = 0, key_bytes = 0, useful_stripes = 0, useful_owners = 0;
        for (const auto& [key, object] : catalog_.keys) {
            key_bytes += key.size();
            // Single object: ID64, slot32, length32; no count or duplicate length.
            useful_objects += key.size() + object.extents.size() * (8 + 4 + 4) + (object.extents.size() == 1 ? 0 : 4);
        }
        for (const auto& [id, stripe] : catalog_.stripes) {
            (void)id;
            // Directory ID, physical version, start/k/stride, optional epoch/mask.
            useful_stripes += 8 + 8 + 2 + 2 + 4 + (WithGc ? 8 : 0) + 4 + stripe.lengths.size() * 4;
            useful_owners += stripe.owners.size() * 8; // Semantic key references, not tree links.
        }
        uint64_t useful_buffers = catalog_.buffer_order.size() * 8 + local_buffers_.size() * 8;
        for (const auto& [id, buffer] : catalog_.buffers) {
            (void)id;
            useful_buffers += 8 + 8 + 4 + 4 + buffer.replicas.size() * 4;
            useful_owners += buffer.owners.size() * 8;
        }
        uint64_t useful_retired = 0;
        for (const auto& [id, replicas] : catalog_.retired_buffers) {
            (void)id; useful_retired += 8 + replicas.size() * 4;
        }
        auto count_retired = [&](const auto& records) {
            for (const auto& [id, stripe] : records) {
                (void)id; useful_retired += 8 + 4 + 4 + 4 + stripe.stored_lengths.size() * 4;
            }
        };
        count_retired(catalog_.retired_stripes); count_retired(gc_intent_);
        uint64_t useful_gc = (gc_index_.dirty.size() + gc_index_.empty.size()) * 8;
        const std::map<std::string, uint64_t> rb = {
            {"objects", mm::rb_tree_fields(catalog_.keys)},
            {"stripes", mm::rb_tree_fields(catalog_.stripes)},
            {"buffers", mm::rb_tree_fields(catalog_.buffers) + mm::rb_tree_fields(local_buffers_)},
            {"retirement", mm::rb_tree_fields(catalog_.retired_buffers) + mm::rb_tree_fields(catalog_.retired_stripes) + mm::rb_tree_fields(gc_intent_)},
            {"gc_candidates", mm::rb_tree_fields(gc_index_.dirty) + mm::rb_tree_fields(gc_index_.empty)},
            {"free_space", 0}};
        return mm::with_rb_tree(mm::with_useful(mm::report({{"objects", objects}, {"stripes", stripes}, {"owners", owners},
                           {"placement_counters", 0}, {"gc_candidates", gc}, {"space_counters", mm::vector(gc_index_.node_bytes)}, {"buffers", buffers}, {"retirement", retired}}, catalog_.keys.size()),
            {{"objects", useful_objects}, {"stripes", useful_stripes}, {"owners", useful_owners},
             {"placement_counters", 0}, {"gc_candidates", useful_gc}, {"space_counters", gc_index_.node_bytes.size() * 8}, {"buffers", useful_buffers}, {"retirement", useful_retired}}, catalog_.keys.size(), key_bytes), rb);
    }
    std::string pack(const Catalog& catalog) const {
        std::ostringstream out;
        out << (catalog.object_layout ? "ECKV10 " : "ECKV9 ") << k_ << ' ' << m_ << ' ' << bytes_ << ' ' << catalog.tail << ' '
            << catalog.used << ' ' << catalog.stripes.size() << ' ' << catalog.keys.size() << ' ' << catalog.buffers.size() << ' ' << catalog.retired_buffers.size();
        out << ' ' << catalog.retired_stripes.size();
        out << '\n';
        for (const auto& [id, stripe] : catalog.stripes) {
            out << id << ' ' << stripe.version << ' ' << stripe.start << ' ' << stripe.data_blocks << ' ' << stripe.block_bytes;
            for (auto length : stripe.lengths) out << ' ' << length;
            out << '\n';
        }
        for (const auto& id : catalog.buffer_order) {
            const auto& buffer = catalog.buffers.at(id);
            out << id << ' ' << buffer.version << ' ' << buffer.used << ' ' << buffer.bytes << ' ' << buffer.replicas.size();
            for (int node : buffer.replicas) out << ' ' << node;
            out << '\n';
        }
        for (const auto& [version, replicas] : catalog.retired_buffers) {
            out << version << ' ' << replicas.size();
            for (int node : replicas) out << ' ' << node;
            out << '\n';
        }
        for (const auto& [version, old] : catalog.retired_stripes) {
            out << version << ' ' << old.start << ' ' << old.count << ' ' << old.bytes;
            if (catalog.object_layout) for (int i = 0; i < old.count; ++i) out << ' ' << old.stored(i);
            out << '\n';
        }
        for (const auto& [key, object] : catalog.keys) {
            out << hex(key) << ' ' << object.size() << ' ' << object.extents.size() << '\n';
            for (const auto& extent : object.extents)
                out << extent.stripe << ' ' << extent_offset(catalog, extent) << ' ' << extent.length << '\n';
        }
        auto payload = out.str();
        return payload;
    }
    static std::string decode_key(const std::string& key) {
        // Called only after validating the legacy snapshot's lowercase hex key.
        auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
        std::string result(key.size() / 2, '\0');
        for (size_t i = 0; i < result.size(); ++i)
            result[i] = static_cast<char>((digit(key[2 * i]) << 4) | digit(key[2 * i + 1]));
        return result;
    }
    Catalog unpack_catalog(const std::string& raw) const {
        std::istringstream in(raw);
        Catalog catalog;
        std::string magic;
        int k = 0, m = 0, bytes = 0;
        size_t stripes = 0, keys = 0, buffers = 0, retired = 0, retired_stripes = 0;
        in >> magic >> k >> m >> bytes >> catalog.tail >> catalog.used >> stripes >> keys;
        catalog.object_layout = (magic == "ECKV8" || magic == "ECKV10");
        in >> buffers;
        in >> retired;
        in >> retired_stripes;
        if (!in || (magic != "ECKV8" && magic != "ECKV9" && magic != "ECKV10") || k != k_ || m != m_ || bytes != bytes_ || catalog.used > capacity() ||
            stripes > raw.size() / 32 || buffers > raw.size() / 32 || retired > raw.size() / 32 || retired_stripes > raw.size() / 32 || keys > raw.size() / 4) throw std::runtime_error("invalid catalog header");
        for (size_t i = 0; i < stripes; ++i) {
            StripeId id;
            Stripe stripe;
            in >> id >> stripe.version >> stripe.start;
            stripe.data_blocks = k_;
            in >> stripe.data_blocks;
            if (!in || stripe.data_blocks < 1 || stripe.data_blocks > k_ || !valid_id(id) || !valid_id(stripe.version) || stripe.start >= static_cast<int>(nodes_.size()))
                throw std::runtime_error("invalid stripe metadata");
            stripe.block_bytes = bytes_;
            stripe.lengths.assign(stripe.data_blocks, bytes_);
            {
                in >> stripe.block_bytes;
                for (auto& length : stripe.lengths) in >> length;
            }
            if (!in || stripe.block_bytes < 64 || stripe.block_bytes > static_cast<int>(MAX_BLOCK) ||
                std::any_of(stripe.lengths.begin(), stripe.lengths.end(), [&](size_t length) {
                    return (!catalog.object_layout && !length) || length > static_cast<size_t>(stripe.block_bytes);
                })) throw std::runtime_error("invalid stripe block lengths");
            if (!in || !catalog.stripes.emplace(id, std::move(stripe)).second) throw std::runtime_error("invalid stripe entry");
        }
        auto read_replicas = [&]() {
            size_t count = 0;
            in >> count;
            if (!in || (count != 0 && count != static_cast<size_t>(m_))) throw std::runtime_error("invalid buffer replica count");
            std::vector<int> replicas;
            std::set<int> unique;
            for (size_t j = 0; j < count; ++j) {
                int node = -1;
                in >> node;
                if (!in || node < 0 || node >= static_cast<int>(nodes_.size()) || !unique.insert(node).second)
                    throw std::runtime_error("invalid buffer replica node");
                replicas.push_back(node);
            }
            return replicas;
        };
        for (size_t i = 0; i < buffers; ++i) {
            StripeId id;
            BufferedBlock buffer;
            in >> id >> buffer.version >> buffer.used;
            buffer.bytes = bytes_;
            in >> buffer.bytes;
            buffer.replicas = read_replicas();
            if (!in || !valid_id(id) || !valid_id(buffer.version) || (!catalog.object_layout && buffer.used == 0) || buffer.bytes == 0 || buffer.used > buffer.bytes || buffer.bytes > MAX_BLOCK ||
                catalog.stripes.count(id) || !catalog.buffers.emplace(id, buffer).second)
                throw std::runtime_error("invalid buffered block");
            catalog.buffer_order.push_back(id);
        }
        for (size_t i = 0; i < retired; ++i) {
            StripeId version;
            in >> version;
            auto replicas = read_replicas();
            if (!valid_id(version) || !catalog.retired_buffers.emplace(version, std::move(replicas)).second)
                throw std::runtime_error("invalid retired buffer");
        }
        for (size_t i = 0; i < retired_stripes; ++i) {
            StripeId version;
            RetiredStripe old;
            in >> version >> old.start >> old.count >> old.bytes;
            if (magic == "ECKV10") {
                require(old.count > 0 && old.count <= k_ + m_, "invalid retired width");
                old.stored_lengths.resize(old.count);
                for (auto& length : old.stored_lengths) { in >> length; require(length <= static_cast<size_t>(old.bytes), "invalid retired length"); }
            }
            if (!in || !valid_id(version) || old.start < 0 || old.start >= static_cast<int>(nodes_.size()) ||
                old.count < m_ + 1 || old.count > k_ + m_ || old.bytes < 64 || old.bytes > static_cast<int>(MAX_BLOCK) ||
                !catalog.retired_stripes.emplace(version, old).second) throw std::runtime_error("invalid retired stripe");
        }
        for (const auto& [id, stripe] : catalog.stripes)
            if (catalog.retired_stripes.count(stripe.version)) throw std::runtime_error("active stripe marked retired");
        for (const auto& [id, buffer] : catalog.buffers)
            if (catalog.retired_buffers.count(buffer.version)) throw std::runtime_error("active buffer marked retired");
        std::set<std::pair<StripeId, size_t>> occupied;
        for (size_t i = 0; i < keys; ++i) {
            std::string key;
            Object object;
            size_t count = 0, length = 0;
            size_t object_bytes = 0;
            in >> key >> object_bytes >> count;
            if (!in || key.empty() || key.size() > 2 * MAX_KEY_BYTES || key.size() % 2 ||
                key.find_first_not_of("0123456789abcdef") != std::string::npos || object_bytes > MAX_VALUE ||
                (catalog.object_layout ? count != 1 : count > object_bytes)) throw std::runtime_error("invalid object metadata");
            for (size_t j = 0; j < count; ++j) {
                Extent extent;
                in >> extent.stripe >> extent.offset >> extent.length;
                size_t limit = 0;
                if (catalog.stripes.count(extent.stripe)) limit = static_cast<size_t>(catalog.stripes.at(extent.stripe).data_blocks) * catalog.stripes.at(extent.stripe).block_bytes;
                else if (catalog.buffers.count(extent.stripe)) limit = catalog.buffers.at(extent.stripe).used;
                if (!in || (!catalog.stripes.count(extent.stripe) && !catalog.buffers.count(extent.stripe)) ||
                    extent.offset > limit || (!catalog.object_layout && !extent.length) || extent.length > limit - extent.offset)
                    throw std::runtime_error("invalid object extent");
                if (catalog.object_layout) {
                    size_t index = 0, expected = 0;
                    if (catalog.stripes.count(extent.stripe)) {
                        const auto& stripe = catalog.stripes.at(extent.stripe);
                        index = extent.offset / stripe.block_bytes;
                        if (extent.offset % stripe.block_bytes || index >= stripe.lengths.size())
                            throw std::runtime_error("invalid object block position");
                        expected = stripe.lengths[index];
                        extent.offset = index;
                    } else {
                        if (extent.offset) throw std::runtime_error("invalid object buffer offset");
                        expected = catalog.buffers.at(extent.stripe).used;
                    }
                    const bool retained = strategy_ == "inplace" && catalog.stripes.count(extent.stripe);
                    if ((retained ? extent.length > expected : extent.length != expected) ||
                        !occupied.emplace(extent.stripe, index).second)
                        throw std::runtime_error("invalid object block ownership or logical/physical length");
                }
                length += extent.length;
                object.extents.push_back(std::move(extent));
            }
            if (length != object_bytes || !catalog.keys.emplace(decode_key(key), std::move(object)).second)
                throw std::runtime_error("invalid object length or duplicate key");
        }
        if ((catalog.tail == "-" && catalog.used != 0) ||
            (catalog.tail != "-" && (!catalog.stripes.count(catalog.tail) || catalog.stripes.at(catalog.tail).data_blocks != k_ || catalog.stripes.at(catalog.tail).block_bytes != bytes_))) throw std::runtime_error("invalid active stripe");
        if (catalog.object_layout) {
            if (catalog.tail != "-" || catalog.used) throw std::runtime_error("object catalog has packing tail");
            for (const auto& [id, buffer] : catalog.buffers)
                if (!occupied.count({id, 0})) throw std::runtime_error("unowned object buffer");
        }
        std::string extra;
        if (in >> extra) throw std::runtime_error("trailing catalog bytes");
        return catalog;
    }
    static size_t extent_offset(const Catalog& catalog, const Extent& extent) {
        auto stripe = catalog.stripes.find(extent.stripe);
        return catalog.object_layout && stripe != catalog.stripes.end()
            ? size_t(extent.offset) * stripe->second.block_bytes : extent.offset;
    }
    static std::vector<const std::string*>& members(Catalog& catalog, const StripeId& id) {
        auto stripe = catalog.stripes.find(id);
        return stripe != catalog.stripes.end() ? stripe->second.owners : catalog.buffers.at(id).owners;
    }
    void publish(Catalog next) {
        // The Store lock publishes all offsets and versions together in memory.
        catalog_ = std::move(next);
        gc_wake_ = true;
        gc_cv_.notify_one();
    }
    void commit(Catalog next) {
        for (auto& [id, stripe] : next.stripes) {
            stripe.owners.clear();
            auto old = catalog_.stripes.find(id);
            stripe.epoch = old == catalog_.stripes.end() ? 0 : old->second.epoch;
        }
        for (auto& [id, buffer] : next.buffers) buffer.owners.clear();
        if (next.object_layout)
            for (const auto& [key, object] : next.keys)
                if (strategy_ != "inplace" || !next.stripes.count(object.extents.at(0).stripe))
                    members(next, object.extents.at(0).stripe).push_back(&key);
        if (replica_cleanup_) {
            // Preserve outstanding cleanup records across legacy publication.
            for (const auto& item : catalog_.retired_buffers) next.retired_buffers.insert(item);
            for (const auto& [id, old] : catalog_.buffers)
                if (!next.buffers.count(id) || next.buffers.at(id).version != old.version)
                    next.retired_buffers[old.version] = old.replicas;
            for (const auto& [id, buffer] : next.buffers) next.retired_buffers.erase(buffer.version);
        }
        for (const auto& item : catalog_.retired_stripes) next.retired_stripes.insert(item);
        for (const auto& [id, old] : catalog_.stripes) {
            if (!next.stripes.count(id) || next.stripes.at(id).version != old.version) {
                if (next.stripes.count(id)) ++next.stripes.at(id).epoch;
                if (next.object_layout && (strategy_ == "fastgc" || strategy_ == "foreground_regroup"))
                    next.retired_stripes[old.version] = retired(old);
            }
        }
        for (const auto& [key, old] : catalog_.keys) {
            auto it = next.keys.find(key);
            bool changed = it == next.keys.end();
            if (!changed) {
                const auto& object = it->second;
                changed = old.size() != object.size() || old.extents.size() != object.extents.size();
                for (size_t i = 0; !changed && i < old.extents.size(); ++i)
                    changed = old.extents[i].stripe != object.extents[i].stripe || old.extents[i].offset != object.extents[i].offset || old.extents[i].length != object.extents[i].length;
            }
            if (changed) for (const auto& extent : old.extents)
                if (next.stripes.count(extent.stripe)) ++next.stripes.at(extent.stripe).epoch;
        }
        for (const auto& [id, stripe] : next.stripes) next.retired_stripes.erase(stripe.version);
        StripeAccounting rebuilt(nodes_.size(), m_);
        uint64_t logical = 0;
        if (next.object_layout) {
            for (auto& [id, stripe] : next.stripes) { stripe.live_mask = 0; }
            for (const auto& [key, object] : next.keys) {
                logical += object.size();
                const auto& e = object.extents.at(0);
                auto stripe = next.stripes.find(e.stripe);
                if (stripe != next.stripes.end()) {
                    stripe->second.live_mask |= uint32_t(1) << (e.offset);
                    rebuilt.logical += e.length;
                }
            }
            for (const auto& [id, stripe] : next.stripes) {
                rebuilt.account(stripe, true);
                if (strategy_ == "fastgc") {
                    if (!stripe.live_mask) rebuilt.empty.insert(id);
                    else if (__builtin_popcount(stripe.live_mask) < stripe.data_blocks) rebuilt.dirty.insert(id);
                }
            }
        }
        publish(std::move(next));
        gc_index_ = std::move(rebuilt); logical_live_bytes_ = logical;
        if (catalog_.object_layout) observe_gc_space();
    }
    // An object mutation stages only its source stripe and current buffer
    // members. It never copies the full foreground catalog. Store mutex_ stays
    // held through I/O/publication; background GC snapshots own their records.
    struct ObjectEdit {
        Catalog next;
        std::set<StripeId> stripes, buffers;
        std::set<std::string> keys;
        bool buffers_included = false;
    };
    ObjectEdit object_edit(const std::string& key, bool source_stripe, bool stage_buffer = true) {
        ObjectEdit edit;
        edit.next.object_layout = true;
        edit.buffers_included = true;
        edit.next.buffer_order = catalog_.buffer_order;
        auto found = catalog_.keys.find(key);
        bool buffered = found != catalog_.keys.end() && catalog_.buffers.count(found->second.extents.at(0).stripe);
        // Existing buffered objects do not increase queue length. Only copy
        // other buffer metadata when this operation can actually seal K items.
        bool sealing = stage_buffer && catalog_.buffer_order.size() + (buffered ? 0 : 1) >= static_cast<size_t>(k_);
        if (key.empty() || sealing) edit.next.buffers = catalog_.buffers;
        else if (buffered) {
            const auto& id = found->second.extents.at(0).stripe;
            edit.next.buffers.emplace(id, catalog_.buffers.at(id));
        }
        for (const auto& [id, buffer] : edit.next.buffers) edit.buffers.insert(id);
        if (source_stripe && found != catalog_.keys.end()) {
            const auto& id = found->second.extents.at(0).stripe;
            auto stripe = catalog_.stripes.find(id);
            if (stripe != catalog_.stripes.end()) {
                edit.next.stripes.emplace(*stripe);
                edit.stripes.insert(id);
            }
        }
        // Range lookups visit only owners of the affected stripe/buffers.
        auto add_key = [&](const std::string& id) {
            if (edit.keys.insert(id).second) edit.next.keys.emplace(id, catalog_.keys.at(id));
        };
        auto add_owners = [&](const StripeId& source) {
            for (const auto* owner : members(catalog_, source)) {
                ++foreground_owner_visits_;
                add_key(*owner);
            }
        };
        if (found != catalog_.keys.end()) add_key(found->first);
        if (strategy_ != "inplace") for (const auto& id : edit.stripes) add_owners(id);
        for (const auto& id : edit.buffers) add_owners(id);
        return edit;
    }
    template<class Map> static void install_records(Map& target, Map& changes) {
        while (!changes.empty()) {
            auto node = changes.extract(changes.begin());
            target.erase(node.key());
            target.insert(std::move(node)); // Same allocator; no new allocation.
        }
    }
    void commit_object(ObjectEdit edit) {
        auto& next = edit.next;
        std::set<StripeId> changed_stripes;
        for (const auto& id : edit.stripes) {
            const auto& old = catalog_.stripes.at(id);
            auto it = next.stripes.find(id);
            if (it == next.stripes.end() || it->second.version != old.version) {
                next.retired_stripes.emplace(old.version, retired(old));
                if (it != next.stripes.end()) changed_stripes.insert(id);
            }
        }
        for (const auto& id : edit.keys) {
            const auto& old = catalog_.keys.at(id);
            auto it = next.keys.find(id);
            bool changed = it == next.keys.end();
            if (!changed) {
                const auto& a = old.extents.at(0);
                const auto& b = it->second.extents.at(0);
                changed = old.size() != it->second.size() || a.stripe != b.stripe || a.offset != b.offset || a.length != b.length;
            }
            const auto& source = old.extents.at(0).stripe;
            if (changed && catalog_.stripes.count(source) &&
                (!edit.stripes.count(source) || next.stripes.count(source))) changed_stripes.insert(source);
        }
        if (replica_cleanup_) for (const auto& id : edit.buffers) {
            const auto& old = catalog_.buffers.at(id);
            auto it = next.buffers.find(id);
            if (it == next.buffers.end() || it->second.version != old.version)
                next.retired_buffers.emplace(old.version, old.replicas);
        }
        // Rebuild only affected member lists. Retained pointers address live
        // map nodes; replacement pointers address staged nodes transferred at
        // publication. No borrowed pointer is dereferenced after its key dies.
        struct MemberEdit { size_t count = 0; std::vector<const std::string*> list; };
        std::map<StripeId, MemberEdit> owner_changes;
        auto needs_members = [&](const StripeId& id) {
            return strategy_ != "inplace" || (!catalog_.stripes.count(id) && !next.stripes.count(id));
        };
        for (const auto& id : edit.keys) {
            const auto& source = catalog_.keys.at(id).extents.at(0).stripe;
            if (needs_members(source)) owner_changes.try_emplace(source);
        }
        for (const auto& [key, object] : next.keys) {
            const auto& target = object.extents.at(0).stripe;
            if (needs_members(target)) ++owner_changes[target].count;
        }
        for (auto& [id, change] : owner_changes) {
            auto retained = [&](const auto* key) { return !edit.keys.count(*key) && !next.keys.count(*key); };
            const auto* old = catalog_.stripes.count(id) || catalog_.buffers.count(id) ? &members(catalog_, id) : nullptr;
            if (old) for (const auto* key : *old) change.count += retained(key);
            change.list.reserve(change.count); // One exact allocation per affected group.
            if (old) for (const auto* key : *old) if (retained(key)) change.list.push_back(key);
        }
        for (const auto& [key, object] : next.keys) {
            const auto& target = object.extents.at(0).stripe;
            if (needs_members(target)) owner_changes.at(target).list.push_back(&key);
        }
        // Preserve deterministic key order for GC and foreground regrouping.
        // Only affected groups are visited; a group has at most K objects.
        for (auto& [id, change] : owner_changes)
            std::sort(change.list.begin(), change.list.end(), [](auto a, auto b) { return *a < *b; });
        for (auto& [id, stripe] : next.stripes) {
            auto old = catalog_.stripes.find(id);
            stripe.epoch = (old == catalog_.stripes.end() ? 0 : old->second.epoch) + changed_stripes.count(id);
        }
        // Stage GC summaries only for affected EC stripes. Existing versions
        // keep their cached masks; remove/add just the edited object slots.
        StripeAccounting::Edit gc_edit;
        gc_edit.affected = edit.stripes;
        for (const auto& [id, stripe] : next.stripes) gc_edit.affected.insert(id);
        uint64_t logical = logical_live_bytes_, ec_logical = gc_index_.logical;
        for (const auto& id : edit.keys) {
            const auto& old = catalog_.keys.at(id);
            logical -= old.size();
            if (catalog_.stripes.count(old.extents.at(0).stripe)) {
                gc_edit.affected.insert(old.extents.at(0).stripe); ec_logical -= old.size();
            }
        }
        for (const auto& [key, object] : next.keys) {
            logical += object.size();
            const auto& id = object.extents.at(0).stripe;
            if (catalog_.stripes.count(id) || next.stripes.count(id)) {
                gc_edit.affected.insert(id); ec_logical += object.size();
            }
        }
        std::set<StripeId> retained_gc_entries;
        for (const auto& id : gc_edit.affected) {
            auto fresh = next.stripes.find(id);
            if (fresh == next.stripes.end() && edit.stripes.count(id)) continue;
            auto old = catalog_.stripes.find(id);
            const auto& stripe = fresh == next.stripes.end() ? old->second : fresh->second;
            if (old != catalog_.stripes.end() && stripe.version == old->second.version) {
                auto entry = StripeAccounting::Entry{old->second.data_blocks, old->second.block_bytes,
                    old->second.live_mask};
                // InPlace may change the coding stride while retaining the
                // physical shard names and slot coefficients.
                if (strategy_ == "inplace") {
                    entry.bytes = stripe.block_bytes;
                }
                gc_edit.entries.emplace(id, std::move(entry));
                retained_gc_entries.insert(id);
            } else gc_edit.entries.emplace(id, StripeAccounting::Entry{stripe.data_blocks, stripe.block_bytes, 0});
        }
        for (const auto& key : edit.keys) {
            const auto& e = catalog_.keys.at(key).extents.at(0);
            if (!retained_gc_entries.count(e.stripe)) continue;
            auto& entry = gc_edit.entries.at(e.stripe);
            entry.mask &= ~(uint32_t(1) << (e.offset));
        }
        for (const auto& [key, object] : next.keys) {
            const auto& e = object.extents.at(0);
            auto entry = gc_edit.entries.find(e.stripe);
            if (entry != gc_edit.entries.end()) {
                entry->second.mask |= uint32_t(1) << (e.offset);
            }
        }
        gc_edit.prepare(strategy_ == "fastgc");
        // Subtract the old directory records before replacing any of them.
        // Everything below is non-allocating, protected by mutex_.
        for (const auto& id : gc_edit.affected) {
            auto old = catalog_.stripes.find(id);
            if (old != catalog_.stripes.end()) gc_index_.account(old->second, false);
        }
        // All allocating work is complete. Publish under the Store lock using
        // node handles, erases and swaps only. Other catalog entries stay put.
        if (edit.buffers_included) {
            foreground_keys_max_ = std::max(foreground_keys_max_, std::max(edit.keys.size(), next.keys.size()));
            foreground_stripes_max_ = std::max(foreground_stripes_max_, std::max(edit.stripes.size(), next.stripes.size()));
        }
        for (const auto& [id, buffer] : next.buffers) catalog_.retired_buffers.erase(buffer.version);
        for (const auto& [id, stripe] : next.stripes) catalog_.retired_stripes.erase(stripe.version);
        // Detach all borrowed references before replacing any owning key node.
        for (const auto& [id, change] : owner_changes) {
            (void)change;
            if (catalog_.stripes.count(id) || catalog_.buffers.count(id)) members(catalog_, id).clear();
        }
        for (const auto& id : changed_stripes)
            if (!next.stripes.count(id)) ++catalog_.stripes.at(id).epoch;
        for (const auto& id : edit.keys) if (!next.keys.count(id)) catalog_.keys.erase(id);
        for (const auto& id : edit.buffers) if (!next.buffers.count(id)) catalog_.buffers.erase(id);
        for (const auto& id : edit.stripes) if (!next.stripes.count(id)) {
            catalog_.stripes.erase(id);
        }
        install_records(catalog_.keys, next.keys);
        install_records(catalog_.buffers, next.buffers);
        install_records(catalog_.stripes, next.stripes);
        install_records(catalog_.retired_buffers, next.retired_buffers);
        install_records(catalog_.retired_stripes, next.retired_stripes);
        if (edit.buffers_included) catalog_.buffer_order.swap(next.buffer_order);
        for (auto& [id, change] : owner_changes) {
            if (catalog_.stripes.count(id) || catalog_.buffers.count(id)) members(catalog_, id).swap(change.list);
            else if (!change.list.empty()) std::terminate(); // No owner may target a removed stripe.
        }
        for (const auto& [id, entry] : gc_edit.entries) {
            auto& stripe = catalog_.stripes.at(id);
            stripe.live_mask = entry.mask;
            gc_index_.account(stripe, true);
        }
        gc_index_.publish_candidates(gc_edit); gc_index_.logical = ec_logical; logical_live_bytes_ = logical;
        observe_gc_space();
        gc_wake_ = true; gc_cv_.notify_one();
    }
    int node_for(const Stripe& stripe, int index) const { return (stripe.start + index) % nodes_.size(); }
    std::future<Frames> node_submit(int node, Frames request, int timeout = 0, RpcTrafficGroup* traffic = nullptr) {
        request.insert(request.begin() + 1, std::to_string(node));
        return node_requests_->submit(node, std::move(request), timeout ? timeout : timeout_, traffic ? traffic->node(node) : nullptr);
    }
    static Frames node_reply(int node, std::future<Frames>& task) {
        auto reply = task.get();
        if (reply.size() != 2) throw Error("UNAVAILABLE", "invalid node response");
        if (reply[0] != "OK") throw Error("UNAVAILABLE", "node " + std::to_string(node) + ": " + reply[1]);
        return reply;
    }
    Frames node_rpc(int node, Frames request, int timeout = 0, RpcTrafficGroup* traffic = nullptr) {
        auto task = node_submit(node, std::move(request), timeout, traffic);
        return node_reply(node, task);
    }
    static size_t stored_bytes(const Stripe& stripe, int index) {
        return index < stripe.data_blocks ? stripe.lengths.at(index) :
            *std::max_element(stripe.lengths.begin(), stripe.lengths.end());
    }
    RetiredStripe retired(const Stripe& stripe) const {
        RetiredStripe old{stripe.start, width(stripe), stripe.block_bytes};
        for (int i = 0; i < width(stripe); ++i) old.stored_lengths.push_back(stored_bytes(stripe, i));
        return old;
    }
    uint64_t stored_total(const Stripe& stripe) const {
        uint64_t total = 0;
        for (int i = 0; i < width(stripe); ++i) total += stored_bytes(stripe, i);
        return total;
    }
    std::string block_name(const Stripe& stripe, int index) const { return stripe.version + "_" + std::to_string(index); }
    static size_t range_bytes(const Stripe& stripe, int index, size_t offset, size_t length) {
        const auto stored = stored_bytes(stripe, index);
        return offset < stored ? std::min(length, stored - offset) : 0;
    }
    Frames shard_range_request(const Stripe& stripe, int index, size_t offset, size_t length) const {
        return {"READ_RANGES", pack_record({block_name(stripe, index), std::to_string(stored_bytes(stripe, index)),
            std::to_string(offset), std::to_string(length)})};
    }
    // Read only the encoded overlap. Bytes beyond the recorded physical length
    // are virtual zeros, unlike missing bytes within an existing codeword.
    std::string read_shard_range(const Stripe& stripe, int index, size_t offset, size_t length) {
        const auto count = range_bytes(stripe, index, offset, length);
        std::string value;
        if (count) {
            value = node_rpc(node_for(stripe, index), shard_range_request(stripe, index, offset, count))[1];
            if (value.size() != count) throw Error("UNAVAILABLE", "short shard range");
        }
        value.resize(length, '\0');
        return value;
    }
    std::string recover_shard_range(const Stripe& stripe, int target, size_t offset, size_t length,
                                   RpcTrafficGroup* traffic = nullptr, uint64_t* read_bytes = nullptr) {
        require(target >= 0 && target < width(stripe) && offset <= size_t(stripe.block_bytes) &&
                length <= size_t(stripe.block_bytes) - offset && length <= MAX_VALUE, "invalid shard recovery range");
        if (!length) return {};
        std::vector<int> candidates;
        for (int i = 0; i < width(stripe); ++i) if (i != target) candidates.push_back(i);
        // Prefer shorter stored overlaps; virtual-zero helpers require no RPC.
        std::stable_sort(candidates.begin(), candidates.end(), [&](int a, int b) {
            return range_bytes(stripe, a, offset, length) < range_bytes(stripe, b, offset, length);
        });
        std::vector<std::string> blocks(width(stripe));
        size_t next = 0;
        int good = 0;
        while (good < stripe.data_blocks && next < candidates.size()) {
            PendingReplies pending(stripe.data_blocks - good);
            std::vector<int> indices;
            while (good + int(indices.size()) < stripe.data_blocks && next < candidates.size()) {
                int i = candidates[next++];
                auto count = range_bytes(stripe, i, offset, length);
                if (!count) { blocks[i].assign(length, '\0'); ++good; continue; }
                pending.add(node_submit(node_for(stripe, i), shard_range_request(stripe, i, offset, count), 0, traffic));
                indices.push_back(i);
            }
            size_t n = 0;
            for (auto& task : pending) {
                int i = indices[n++];
                try {
                    auto reply = node_reply(node_for(stripe, i), task);
                    if (read_bytes) *read_bytes += reply[1].size();
                    if (reply[1].size() != range_bytes(stripe, i, offset, length)) continue;
                    blocks[i] = std::move(reply[1]); blocks[i].resize(length, '\0'); ++good;
                } catch (const std::exception&) { /* Try spare helpers in the next wave. */ }
            }
        }
        // Local codec also makes this helper safe for background GC snapshots;
        // it never accesses the foreground codec cache or mutable catalog.
        Codec codec(stripe.data_blocks, m_, stripe.block_bytes);
        return codec.recover_target(blocks, target, length);
    }
    void write_shards(const Stripe& stripe, const std::vector<std::string>& blocks, const std::vector<int>& indices, RpcTrafficGroup* traffic = nullptr) {
        // Jobs own their payloads. Drain accepted jobs even if a later submit or
        // reply fails, before the caller may retire an unpublished version.
        PendingReplies tasks(indices.size());
        for (int i : indices)
            tasks.add(node_submit(node_for(stripe, i), {"WRITE", block_name(stripe, i), blocks[i].substr(0, stored_bytes(stripe, i))}, 0, traffic));
        std::string error;
        size_t n = 0;
        for (auto& task : tasks) {
            int node = node_for(stripe, indices[n++]);
            try { node_reply(node, task); } catch (const std::exception& e) { error = e.what(); }
        }
        if (!error.empty()) throw Error("UNAVAILABLE", "shard write failed: " + error);
    }
    void read_shard_indices(const Stripe& stripe, const std::vector<int>& indices,
                            std::vector<std::string>& blocks, RpcTrafficGroup* traffic = nullptr) {
        PendingReplies tasks(indices.size());
        for (int i : indices)
            tasks.add(node_submit(node_for(stripe, i), {"READ", block_name(stripe, i)}, 0, traffic));
        size_t n = 0;
        for (auto& task : tasks) {
            int i = indices[n++];
            try {
                auto reply = node_reply(node_for(stripe, i), task);
                if (reply[1].size() == stored_bytes(stripe, i)) {
                    blocks[i] = std::move(reply[1]);
                    blocks[i].resize(stripe.block_bytes, '\0'); // Transient RS workspace only.
                }
            } catch (const std::exception&) { /* Missing, malformed or unreachable shards are erasures. */ }
        }
    }
    std::vector<std::string> read_shards(const Stripe& stripe, RpcTrafficGroup* traffic = nullptr) {
        std::vector<int> indices;
        for (int i = 0; i < width(stripe); ++i) indices.push_back(i);
        std::vector<std::string> blocks(width(stripe));
        read_shard_indices(stripe, indices, blocks, traffic);
        return blocks;
    }
    std::vector<std::string> regroup_data(const Stripe& stripe, int removed) {
        std::vector<int> indices;
        for (int i = 0; i < stripe.data_blocks; ++i) if (i != removed) indices.push_back(i);
        std::vector<std::string> blocks(width(stripe));
        read_shard_indices(stripe, indices, blocks, &regroup_traffic_);
        bool missing = false;
        for (int i : indices) missing |= blocks[i].empty();
        if (!missing) return blocks;
        // Only failed survivor reads require the old codeword. Keep successful
        // reads and fetch the omitted data slot plus parity for RS recovery.
        indices.clear(); indices.push_back(removed);
        for (int i = stripe.data_blocks; i < width(stripe); ++i) indices.push_back(i);
        read_shard_indices(stripe, indices, blocks, &regroup_traffic_);
        return recover(stripe, blocks);
    }
    std::vector<std::string> recover(const Stripe& stripe, const std::vector<std::string>& blocks) {
        auto restored = codec_for(stripe).recover(blocks);
        return restored;
    }
    std::string stripe_data(const Stripe& stripe, RpcTrafficGroup* traffic = nullptr) {
        auto blocks = read_shards(stripe, traffic);
        bool missing = false;
        for (int i = 0; i < stripe.data_blocks; ++i) missing |= blocks[i].empty();
        if (missing) blocks = recover(stripe, blocks);
        std::string data;
        for (int i = 0; i < stripe.data_blocks; ++i) data += blocks[i];
        return data;
    }
    bool valid_buffer(const BufferedBlock& buffer, const std::string& data) const {
        return data.size() == buffer.bytes;
    }
    std::string local_buffer(const BufferedBlock& buffer) const {
        auto it = local_buffers_.find(buffer.version);
        if (it != local_buffers_.end() && valid_buffer(buffer, it->second)) return it->second;
        return {};
    }
    std::vector<std::string> read_buffer_replicas(const BufferedBlock& buffer) {
        PendingReplies pending(buffer.replicas.size());
        for (size_t i = 0; i < buffer.replicas.size(); ++i) {
            int node = buffer.replicas[i];
            pending.add(buffer_writers_.submit(i, nodes_[node], {"BUFFER_READ", std::to_string(node), buffer.version}, nullptr, node));
        }
        std::vector<std::string> copies;
        for (auto& task : pending) {
            std::string data;
            try {
                auto reply = task.get();
                if (valid_buffer(buffer, reply[1])) data = std::move(reply[1]);
            } catch (const std::exception&) { }
            copies.push_back(std::move(data));
        }
        return copies;
    }
    void write_buffer_replicas(const BufferedBlock& buffer, const std::string& data, const std::vector<int>& indices) {
        PendingReplies pending(indices.size());
        // Submit every replica before waiting: one persistent Proxy sender per replica index.
        for (int i : indices) {
            int node = buffer.replicas.at(i);
            pending.add(buffer_writers_.submit(i, nodes_[node], {"BUFFER_WRITE", std::to_string(node), buffer.version, data}, nullptr, node));
        }
        std::string error;
        for (auto& task : pending) {
            try { task.get(); } catch (const std::exception& e) { error = e.what(); }
        }
        if (!error.empty()) throw Error("UNAVAILABLE", "buffer replication failed: " + error);
    }
    void replicate_buffer(BufferedBlock& buffer, const std::string& data) {
        if (buffer.replicas.empty()) {
            size_t start = std::hash<std::string>{}(buffer.version) % nodes_.size();
            for (int i = 0; i < m_; ++i) buffer.replicas.push_back((start + i) % nodes_.size());
        }
        std::vector<int> indices;
        for (int i = 0; i < m_; ++i) indices.push_back(i);
        write_buffer_replicas(buffer, data, indices);
    }
    std::string buffer_data(const BufferedBlock& buffer) {
        auto data = local_buffer(buffer);
        if (!data.empty()) return data;
        auto copies = read_buffer_replicas(buffer);
        for (const auto& copy : copies) {
            if (copy.empty()) continue;
            // GET can still succeed if the local buffer cannot be restored immediately.
            try { local_buffers_.insert_or_assign(buffer.version, copy); }
            catch (const std::exception& e) { std::cerr << "local buffer repair failed: " << e.what() << '\n'; }
            return copy;
        }
        throw Error("UNAVAILABLE", "no valid local buffer or temporary replica");
    }
    void save_buffer(BufferedBlock& buffer, const std::string& data) {
        require(!data.empty() && data.size() <= MAX_BLOCK && buffer.used <= data.size(), "invalid buffered block size");
        buffer.bytes = data.size();
        buffer.version = StripeId::next();
        local_buffers_.insert_or_assign(buffer.version, data);
        replicate_buffer(buffer, data); // Publish only after all M remote in-memory acknowledgments.
    }
    void ensure_buffer_replicas() {
        bool missing = false;
        for (const auto& [id, buffer] : catalog_.buffers) missing |= buffer.replicas.empty();
        if (!missing) return;
        if (catalog_.object_layout) {
            auto edit = object_edit("", false);
            for (auto& [id, buffer] : edit.next.buffers)
                if (buffer.replicas.empty()) replicate_buffer(buffer, buffer_data(buffer));
            commit_object(std::move(edit));
            return;
        }
        Catalog next = catalog_;
        for (auto& [id, buffer] : next.buffers)
            if (buffer.replicas.empty()) replicate_buffer(buffer, buffer_data(buffer));
        commit(std::move(next));
    }
    uint64_t repair_buffer(const BufferedBlock& buffer, int target) {
        auto data = local_buffer(buffer);
        auto copies = read_buffer_replicas(buffer);
        uint64_t repaired = 0;
        if (data.empty()) {
            for (const auto& copy : copies) if (!copy.empty()) { data = copy; break; }
            if (data.empty()) throw Error("UNAVAILABLE", "no valid buffer replica to repair");
            if (target < 0) {
                local_buffers_.insert_or_assign(buffer.version, data);
                ++repaired;
            }
        }
        std::vector<int> missing;
        for (size_t i = 0; i < copies.size(); ++i)
            if (copies[i].empty() && (target < 0 || buffer.replicas[i] == target)) missing.push_back(i);
        write_buffer_replicas(buffer, data, missing);
        return repaired + missing.size();
    }
    void cleanup_buffers() {
        if (!replica_cleanup_) return;
        // Do not reclaim buffer versions while an update is executing.
        if (rmw_active_ || catalog_.retired_buffers.empty()) return;
        // Include the entire newly sealed K-object group. The larger bound only
        // limits a failure backlog; normal foreground calls retire at most K.
        Frames versions;
        std::map<int, Frames> by_node;
        for (const auto& [version, replicas] : catalog_.retired_buffers) {
            if (versions.size() == 4096) break;
            versions.push_back(version);
            for (int node : replicas) by_node[node].push_back(version);
        }
        PendingReplies pending(by_node.size());
        ++buffer_cleanup_rounds_;
        for (const auto& [node, names] : by_node) {
            pending.add(node_requests_->submit(node,
                {"BUFFER_DELETE_BATCH", std::to_string(node), pack_record(names)}, timeout_));
            ++buffer_delete_requests_;
            buffer_delete_items_ += names.size();
        }
        // All node requests are queued before waiting; futures are drained even
        // on exceptions. The Store mutex keeps retirement entries stable.
        std::set<int> failed;
        auto node = by_node.begin();
        for (auto& task : pending) {
            try {
                auto reply = task.get();
                if (reply[0] != "OK") failed.insert(node->first);
            } catch (const std::exception&) { failed.insert(node->first); }
            ++node;
        }
        for (const auto& version : versions) {
            auto current = catalog_.retired_buffers.find(version);
            bool success = true;
            for (int replica : current->second) if (failed.count(replica)) success = false;
            if (success) {
                local_buffers_.erase(version);
                catalog_.retired_buffers.erase(current);
            }
            // Unknown/partial outcomes keep the whole object for idempotent
            // replay; the immutable retirement fence rejects delayed writes.
        }
    }
    void cleanup_kvmd_stripes() {
        // Called only by the foreground with mutex_ held. The in-memory catalog has
        // already switched to the new version before any old shard is deleted.
        if (strategy_ != "foreground_regroup" || !catalog_.object_layout || catalog_.retired_stripes.empty()) return;
        require(!rmw_active_, "cannot reclaim during pending RMW");
        std::string error;
        for (auto it = catalog_.retired_stripes.begin(); it != catalog_.retired_stripes.end();) {
            auto current = it++;
            const auto& [version, old] = *current;
            PendingReplies pending(old.count);
            for (int i = 0; i < old.count; ++i) {
                int node = (old.start + i) % nodes_.size();
                pending.add(node_requests_->submit(node,
                    {"DELETE", std::to_string(node), version + "_" + std::to_string(i)}, timeout_, regroup_traffic_.node(node)));
            }
            bool success = true;
            for (auto& task : pending) {
                try {
                    auto reply = task.get();
                    if (reply[0] != "OK") throw Error("UNAVAILABLE", reply[1]);
                } catch (const std::exception& e) { success = false; error = e.what(); }
            }
            if (success) catalog_.retired_stripes.erase(current);
        }
        if (!error.empty()) throw Error("UNAVAILABLE", "KVMD committed data; old shard cleanup pending: " + error);
    }
    std::string extent_data(const Catalog& catalog, const Extent& extent) {
        if (catalog.buffers.count(extent.stripe)) return buffer_data(catalog.buffers.at(extent.stripe));
        return stripe_data(catalog.stripes.at(extent.stripe));
    }
    void update_rmw(const std::string& key, const std::string& value, bool update) {
        const auto& object = lookup(key);
        require(value.size() == object.size(), "fixed-length RMW requires the existing value length");
        // Check affected blocks before creating intent: known offline/missing nodes leave the old value untouched.
        std::set<std::pair<std::string, int>> touched;
        for (const auto& extent : object.extents) {
            if (catalog_.buffers.count(extent.stripe)) { buffer_data(catalog_.buffers.at(extent.stripe)); continue; }
            if (!extent.length) continue;
            const auto& stripe = catalog_.stripes.at(extent.stripe);
            const auto absolute = extent_offset(catalog_, extent);
            int first = absolute / stripe.block_bytes, last = (absolute + extent.length - 1) / stripe.block_bytes;
            for (int i = first; i <= last; ++i) touched.emplace(extent.stripe, i);
            for (int j = 0; j < m_; ++j) touched.emplace(extent.stripe, stripe.data_blocks + j);
        }
        std::map<int, std::vector<std::string>> by_node;
        for (const auto& item : touched) {
            const auto& stripe = catalog_.stripes.at(item.first);
            by_node[node_for(stripe, item.second)].push_back(block_name(stripe, item.second));
        }
        for (const auto& extent : object.extents) {
            if (!catalog_.buffers.count(extent.stripe)) continue;
            for (int node : catalog_.buffers.at(extent.stripe).replicas) by_node.try_emplace(node);
        }
        // Bound preflight concurrency by node count, even for values spanning many stripes.
        std::vector<std::future<void>> checks;
        for (const auto& group : by_node) checks.push_back(std::async(std::launch::async, [&, node = group.first] {
            auto config = node_rpc(node, {"CONFIG"})[1];
            if (config != std::to_string(k_) + " " + std::to_string(m_) + " " + std::to_string(bytes_))
                throw Error("UNAVAILABLE", "node EC configuration mismatch");
            for (const auto& block : by_node.at(node)) node_rpc(node, {"CHECK", block});
        }));
        std::string error;
        for (auto& check : checks) {
            try { check.get(); } catch (const std::exception& e) { error = e.what(); }
        }
        if (!error.empty()) throw Error("UNAVAILABLE", error);
        // Locations do not change. Use live metadata under the Store lock;
        // only actual buffer payloads need replacement versions.
        struct Active {
            bool& flag;
            explicit Active(bool& f) : flag(f) { flag = true; }
            ~Active() { flag = false; }
        } active(rmw_active_);
        for (const auto& extent : object.extents)
            if (catalog_.stripes.count(extent.stripe)) ++catalog_.stripes.at(extent.stripe).epoch;
        size_t value_offset = 0, sequence = 0;
        const auto request_id = uuid();
        std::map<StripeId, std::string> buffer_values;
        for (const auto& extent : object.extents) {
            auto buffer = catalog_.buffers.find(extent.stripe);
            if (buffer != catalog_.buffers.end()) {
                auto [it, fresh] = buffer_values.try_emplace(extent.stripe);
                if (fresh) it->second = buffer_data(buffer->second);
                it->second.replace(extent.offset, extent.length, value, value_offset, extent.length);
                value_offset += extent.length;
                continue;
            }
            const auto& stripe = catalog_.stripes.at(extent.stripe);
            for (size_t pos = 0; pos < extent.length;) {
                size_t absolute = extent_offset(catalog_, extent) + pos;
                int index = absolute / stripe.block_bytes, offset = absolute % stripe.block_bytes;
                size_t length = std::min(extent.length - pos, static_cast<size_t>(stripe.block_bytes - offset));
                std::ostringstream header;
                header << index << ' ' << offset << ' ' << stripe.data_blocks << ' ' << m_ << ' ' << stripe.block_bytes << '\n';
                for (int j = 0; j < m_; ++j)
                    header << node_for(stripe, stripe.data_blocks + j) << ' ' << block_name(stripe, stripe.data_blocks + j) << '\n';
                node_rpc(node_for(stripe, index), {"RMW", request_id + "_" + std::to_string(sequence++),
                    block_name(stripe, index), header.str(), value.substr(value_offset, length)}, 3 * timeout_ + 1000);
                pos += length; value_offset += length;
            }
        }
        std::map<StripeId, BufferedBlock> replacements;
        std::map<StripeId, std::vector<int>> retired;
        for (const auto& [id, data] : buffer_values) {
            const auto& old = catalog_.buffers.at(id);
            auto& fresh = replacements.emplace(id, old).first->second;
            save_buffer(fresh, data);
            if (replica_cleanup_) retired.emplace(old.version, old.replicas);
        }
        install_records(catalog_.buffers, replacements);
        install_records(catalog_.retired_buffers, retired);
        ++direct_rmw_updates_;
        record_write(update);

    }
    static void append_extent(InlineExtents& extents, Extent extent) {
        if (!extent.length) return;
        if (!extents.empty() && extents.back().stripe == extent.stripe &&
            extents.back().offset + extents.back().length == extent.offset)
            extents.back().length += extent.length;
        else extents.push_back(std::move(extent));
    }
    void store_stripe(Stripe& stripe, const std::string& data, RpcTrafficGroup* traffic = nullptr) {
        stripe.version = StripeId::next();
        auto blocks = codec_for(stripe).encode(data);
        std::vector<int> indices;
        for (int i = 0; i < width(stripe); ++i) {
            indices.push_back(i);
        }
        write_shards(stripe, blocks, indices, traffic);
    }
    uint64_t seal_buffers(Catalog& next, size_t other_stripes = 0) {
        uint64_t groups = 0;
        while (next.buffer_order.size() >= static_cast<size_t>(k_)) {
            int count = k_;
            Stripe stripe;
            stripe.data_blocks = count;
            stripe.start = (other_stripes + next.stripes.size()) % nodes_.size();
            stripe.block_bytes = 64; // ISA-L minimum; independent of the default packing size.
            for (int i = 0; i < count; ++i) {
                size_t length = next.buffers.at(next.buffer_order[i]).used;
                stripe.lengths.push_back(length);
                stripe.block_bytes = std::max(stripe.block_bytes, static_cast<int>(length));
            }
            const auto id = StripeId::next();
            std::string data;
            std::map<StripeId, size_t> offsets;
            for (int i = 0; i < count; ++i) {
                const auto& buffer_id = next.buffer_order[i];
                offsets[buffer_id] = next.object_layout ? size_t(i) : static_cast<size_t>(i) * stripe.block_bytes;
                auto block = buffer_data(next.buffers.at(buffer_id));
                block.resize(stripe.lengths[i]);
                block.resize(stripe.block_bytes, '\0');
                data += block;
            }
            store_stripe(stripe, data);
            next.stripes.emplace(id, std::move(stripe));
            for (auto& [key, object] : next.keys) {
                InlineExtents extents;
                for (auto extent : object.extents) {
                    auto it = offsets.find(extent.stripe);
                    if (it != offsets.end()) { extent.stripe = id; extent.offset += it->second; }
                    if (next.object_layout) extents.push_back(std::move(extent));
                    else append_extent(extents, std::move(extent));
                }
                object.extents = std::move(extents);
            }
            for (int i = 0; i < count; ++i) {
                const auto& buffer = next.buffers.at(next.buffer_order[i]);
                if (replica_cleanup_) next.retired_buffers[buffer.version] = buffer.replicas;
                next.buffers.erase(next.buffer_order[i]);
            }
            next.buffer_order.erase(next.buffer_order.begin(), next.buffer_order.begin() + count);
            ++groups;
        }
        return groups;
    }
    // A buffered object has exactly one extent, including an empty value. A one-byte
    // zero sentinel keeps empty buffers distinguishable from failed replica reads.
    void stage_object(Catalog& next, const std::string& key_hex, const std::string& value) {
        const auto id = StripeId::next();
        BufferedBlock buffer;
        buffer.used = value.size();
        save_buffer(buffer, value.empty() ? std::string(1, '\0') : value);
        next.buffers.emplace(id, std::move(buffer));
        next.buffer_order.push_back(id);
        next.keys[key_hex] = Object{{{id, 0, value.size()}}};
    }
    void erase_buffer(Catalog& next, const StripeId& id) {
        next.buffers.erase(id);
        next.buffer_order.erase(std::remove(next.buffer_order.begin(), next.buffer_order.end(), id), next.buffer_order.end());
    }
    void migrate_objects() {
        if (coding_unit_ != "object" || catalog_.object_layout) return;
        // Build a complete new layout while the old catalog and shards stay readable.
        // Publish only after every new EC shard/temporary replica is acknowledged.
        Catalog next;
        next.object_layout = true;
        for (const auto& [key, object] : catalog_.keys) {
            std::string value;
            for (const auto& extent : object.extents)
                value += extent_data(catalog_, extent).substr(extent.offset, extent.length);
            if (value.size() != object.size())
                throw Error("UNAVAILABLE", "object migration: value length mismatch");
            stage_object(next, key, value);
            seal_buffers(next);
        }
        commit(std::move(next));
    }
    void update_inplace(const std::string& key, const std::string& value, bool update) {
        const auto id = key_id(key);
        const auto& object = lookup(key);
        const auto source = object.extents.at(0).stripe;
        const bool resized = value.size() != object.size();
        auto edit = object_edit(id, true, false); // Only this stripe's owners.
        auto& next = edit.next;
        if (catalog_.buffers.count(source)) {
            // No stripe yet: overwrite the same M replicas, keeping their
            // identities and buffer position. Empty buffers retain the sentinel.
            auto& buffer = next.buffers.at(source);
            auto payload = value.empty() ? std::string(1, '\0') : value;
            buffer.used = value.size(); buffer.bytes = payload.size();
            replicate_buffer(buffer, payload);
            local_buffers_.insert_or_assign(buffer.version, std::move(payload));
            next.keys.at(id) = Object{{{source, 0, value.size()}}};
            commit_object(std::move(edit));
            ++inplace_buffer_updates_;
        } else {
            const auto& old = catalog_.stripes.at(source);
            const int slot = object.extents.at(0).offset;
            auto& stripe = next.stripes.at(source);
            // Existing length slots describe physical codeword bytes. Logical
            // length stays in Object/Extent; no extra per-object index is needed.
            stripe.lengths.at(slot) = std::max<size_t>(old.lengths.at(slot), value.size());
            stripe.block_bytes = std::max<size_t>(64, *std::max_element(stripe.lengths.begin(), stripe.lengths.end()));
            next.keys.at(id).extents.at(0).length = value.size();
            std::ostringstream header;
            header << slot << ' ' << old.data_blocks << ' ' << m_ << ' ' << old.lengths.at(slot)
                   << ' ' << stored_bytes(old, old.data_blocks) << ' ' << stored_bytes(stripe, stripe.data_blocks) << '\n';
            for (int j = 0; j < m_; ++j)
                header << node_for(old, old.data_blocks + j) << ' ' << block_name(old, old.data_blocks + j) << '\n';
            node_rpc(node_for(old, slot), {"RMW_RESIZE", uuid(), block_name(old, slot), header.str(), value}, 3 * timeout_ + 1000);
            commit_object(std::move(edit));
            ++inplace_delta_updates_;
        }
        resizes_ += resized;
        record_write(update);
    }
    void resize_lazy(const std::string& key, const std::string& value, bool update) {
        auto edit = object_edit(key_id(key), false);
        auto& next = edit.next;
        auto id = key_id(key);
        const auto source = next.keys.at(id).extents.at(0).stripe;
        if (next.buffers.count(source)) erase_buffer(next, source);
        // Old encoded bytes and parity remain untouched; liveness follows key extents.
        stage_object(next, id, value);
        auto groups = seal_buffers(next, catalog_.stripes.size() - edit.stripes.size());
        commit_object(std::move(edit));
        ++resizes_;
        regrouped_ += groups;
        record_write(update);
    }
    void resize_object(const std::string& key, const std::string& value, bool update) {
        auto edit = object_edit(key_id(key), true);
        auto& next = edit.next;
        const auto key_hex = key_id(key);
        const auto extent = next.keys.at(key_hex).extents.at(0);
        bool encoded = next.stripes.count(extent.stripe);
        bool reencoded = false;
        if (!encoded) erase_buffer(next, extent.stripe);
        else {
            const auto& old = catalog_.stripes.at(extent.stripe);
            int removed = extent.offset;
            Stripe stripe;
            stripe.data_blocks = old.data_blocks - 1;
            stripe.start = (old.start + 1) % nodes_.size();
            stripe.block_bytes = 64;
            for (int i = 0; i < old.data_blocks; ++i) {
                if (i == removed) continue;
                stripe.lengths.push_back(old.lengths[i]);
                stripe.block_bytes = std::max(stripe.block_bytes, static_cast<int>(old.lengths[i]));
            }
            if (!stripe.data_blocks) next.stripes.erase(extent.stripe);
            else {
                auto old_blocks = regroup_data(old, removed);
                std::string data;
                data.reserve(static_cast<size_t>(stripe.data_blocks) * stripe.block_bytes);
                for (int i = 0; i < old.data_blocks; ++i) {
                    if (i == removed) continue;
                    auto block = std::move(old_blocks[i]);
                    block.resize(old.lengths[i]);
                    block.resize(stripe.block_bytes, '\0');
                    data += block;
                }
                store_stripe(stripe, data, &regroup_traffic_);
                for (auto& [other_key, object] : next.keys) {
                    if (other_key == key_hex) continue;
                    auto& other = object.extents.at(0);
                    if (other.stripe != extent.stripe) continue;
                    int index = other.offset;
                    other.offset = index - (index > removed);
                }
                next.stripes.at(extent.stripe) = std::move(stripe);
                reencoded = true;
            }
        }
        stage_object(next, key_hex, value);
        auto groups = seal_buffers(next, catalog_.stripes.size() - edit.stripes.size());
        commit_object(std::move(edit));
        ++resizes_;
        peeled_ += encoded;
        reencoded_ += reencoded;
        regrouped_ += groups;
        record_write(update);
    }
    void resize_regroup(const std::string& key, const std::string& value, bool update) {
        const auto key_hex = key_id(key);
        const auto& old_object = lookup(key);
        Catalog next = catalog_;
        std::map<StripeId, std::set<int>> removed;
        std::set<StripeId> removed_buffers;
        for (const auto& extent : old_object.extents) {
            if (catalog_.buffers.count(extent.stripe)) { removed_buffers.insert(extent.stripe); continue; }
            const auto& stripe = catalog_.stripes.at(extent.stripe);
            int first = extent.offset / stripe.block_bytes, last = (extent.offset + extent.length - 1) / stripe.block_bytes;
            for (int i = first; i <= last; ++i) removed[extent.stripe].insert(i);
        }
        std::map<StripeId, std::string> source_cache;
        auto source = [&](const StripeId& id) -> const std::string& {
            auto [it, fresh] = source_cache.try_emplace(id);
            if (fresh) it->second = catalog_.buffers.count(id) ? buffer_data(catalog_.buffers.at(id)) : stripe_data(catalog_.stripes.at(id), &regroup_traffic_);
            return it->second;
        };
        std::map<StripeId, std::vector<int>> positions;
        uint64_t peeled = 0, reencoded = 0;
        // Rewrite survivors using their longest logical block, discarding obsolete zero padding.
        for (const auto& [id, indices] : removed) {
            const auto& old = catalog_.stripes.at(id);
            const auto& old_data = source(id);
            auto& mapping = positions[id];
            mapping.assign(old.data_blocks, -1);
            Stripe stripe;
            stripe.block_bytes = 64;
            stripe.start = (old.start + 1) % nodes_.size();
            for (int i = 0; i < old.data_blocks; ++i) {
                if (indices.count(i)) continue;
                mapping[i] = stripe.data_blocks++;
                stripe.lengths.push_back(old.lengths[i]);
                stripe.block_bytes = std::max(stripe.block_bytes, static_cast<int>(old.lengths[i]));
            }
            peeled += indices.size();
            if (next.tail != "-" && StripeId(next.tail) == id) { next.tail = "-"; next.used = 0; }
            if (!stripe.data_blocks) next.stripes.erase(id);
            else {
                std::string remaining;
                for (int i = 0; i < old.data_blocks; ++i) {
                    if (indices.count(i)) continue;
                    auto block = old_data.substr(static_cast<size_t>(i) * old.block_bytes, old.lengths[i]);
                    block.resize(stripe.block_bytes, '\0');
                    remaining += block;
                }
                store_stripe(stripe, remaining, &regroup_traffic_);
                next.stripes[id] = std::move(stripe);
                ++reencoded;
            }
        }
        for (const auto& id : removed_buffers) { source(id); next.buffers.erase(id); }
        next.buffer_order.erase(std::remove_if(next.buffer_order.begin(), next.buffer_order.end(),
            [&](const StripeId& id) { return removed_buffers.count(id); }), next.buffer_order.end());

        struct Fragment { std::string key; size_t logical, absolute, length; };
        using BlockId = std::pair<StripeId, int>;
        std::map<BlockId, std::vector<Fragment>> detached;
        std::map<std::string, std::vector<std::pair<size_t, Extent>>> rebuilt;
        // Track logical value offsets separately from physical block offsets.
        for (const auto& [other_key, object] : catalog_.keys) {
            size_t logical = 0;
            for (const auto& extent : object.extents) {
                bool buffered = catalog_.buffers.count(extent.stripe);
                size_t stride = buffered ? catalog_.buffers.at(extent.stripe).bytes : catalog_.stripes.at(extent.stripe).block_bytes;
                for (size_t pos = 0; pos < extent.length;) {
                    size_t absolute = extent.offset + pos;
                    int index = absolute / stride;
                    size_t offset = absolute % stride;
                    size_t length = std::min(extent.length - pos, stride - offset);
                    bool moved = buffered ? removed_buffers.count(extent.stripe) :
                        (removed.count(extent.stripe) && removed.at(extent.stripe).count(index));
                    if (moved) detached[{extent.stripe, index}].push_back({other_key, logical + pos, absolute, length});
                    else {
                        size_t relocated = absolute;
                        if (removed.count(extent.stripe)) relocated = static_cast<size_t>(positions.at(extent.stripe)[index]) *
                            next.stripes.at(extent.stripe).block_bytes + offset;
                        rebuilt[other_key].push_back({logical + pos, {extent.stripe, relocated, length}});
                    }
                    pos += length;
                }
                logical += extent.length;
            }
        }
        // One detached source block becomes one resized buffer block. Never split at default B.
        for (auto& [block_id, fragments] : detached) {
            std::sort(fragments.begin(), fragments.end(), [](const Fragment& a, const Fragment& b) { return a.absolute < b.absolute; });
            auto id = StripeId::next();
            std::string data;
            for (const auto& fragment : fragments) {
                std::string part;
                if (fragment.key == key_hex) {
                    size_t begin = std::min(fragment.logical, value.size());
                    size_t end = fragment.logical + fragment.length == old_object.size() ? value.size() :
                        std::min(fragment.logical + fragment.length, value.size());
                    part = value.substr(begin, end - begin);
                } else part = source(block_id.first).substr(fragment.absolute, fragment.length);
                if (part.empty()) continue;
                require(part.size() <= MAX_BLOCK - data.size(), "resized block exceeds 32 MiB");
                rebuilt[fragment.key].push_back({fragment.logical, {id, data.size(), part.size()}});
                data += part;
            }
            if (data.empty()) continue;
            BufferedBlock buffer;
            buffer.used = data.size();
            save_buffer(buffer, data);
            next.buffers.emplace(id, buffer);
            next.buffer_order.push_back(id);
        }
        // Growing an empty value has no old block to detach.
        if (!old_object.size() && !value.empty()) {
            auto id = StripeId::next();
            BufferedBlock buffer;
            buffer.used = value.size();
            save_buffer(buffer, value);
            next.buffers.emplace(id, buffer);
            next.buffer_order.push_back(id);
            rebuilt[key_hex].push_back({0, {id, 0, value.size()}});
        }
        for (auto& [other_key, object] : next.keys) {
            auto& pieces = rebuilt[other_key];
            std::sort(pieces.begin(), pieces.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
            object.extents.clear();
            for (auto& piece : pieces) append_extent(object.extents, std::move(piece.second));
        }
        uint64_t groups = seal_buffers(next);
        commit(std::move(next));
        ++resizes_;
        peeled_ += peeled;
        reencoded_ += reencoded;
        regrouped_ += groups;
        record_write(update);
    }
    void put(const std::string& key, const std::string& value, bool update) {
        auto id = key_id(key);
        require(value.size() <= MAX_VALUE, "value exceeds 16 MiB");
        if (update && !catalog_.keys.count(id)) throw Error("NOT_FOUND", "UPDATE requires an existing key");
        if (catalog_.keys.count(id)) {
            if (strategy_ == "inplace") {
                const auto& object = catalog_.keys.at(id);
                if (value.size() == object.size() && catalog_.stripes.count(object.extents.at(0).stripe))
                    update_rmw(key, value, update);
                else update_inplace(key, value, update);
                return;
            }
            if (value.size() == catalog_.keys.at(id).size()) update_rmw(key, value, update);
            else {
                require(strategy_ == "foreground_regroup" || strategy_ == "fastgc", "fixed-length RMW requires the existing value length");
                if (strategy_ == "fastgc") resize_lazy(key, value, update);
                else if (catalog_.object_layout) resize_object(key, value, update);
                else resize_regroup(key, value, update);
            }
            return;
        }
        if (catalog_.object_layout) {
            auto edit = object_edit(id, false);
            stage_object(edit.next, id, value);
            auto groups = seal_buffers(edit.next, catalog_.stripes.size());
            commit_object(std::move(edit));
            regrouped_ += groups;
            record_write(update);
            return;
        }
        Catalog next = catalog_;
        Object object;
        std::vector<int> indices;
        for (int i = 0; i < codec_.width(); ++i) indices.push_back(i);
        for (size_t pos = 0; pos < value.size();) {
            std::string data(capacity(), '\0');
            Stripe stripe;
            stripe.data_blocks = k_;
            stripe.block_bytes = bytes_;
            stripe.lengths.assign(k_, bytes_);
            if (next.tail == "-" || next.used == capacity()) {
                next.tail = std::string(StripeId::next());
                next.used = 0;
                stripe.start = next.stripes.size() % nodes_.size();
            } else {
                stripe = next.stripes.at(next.tail);
                data = stripe_data(stripe);
            }
            size_t length = std::min(value.size() - pos, capacity() - next.used);
            data.replace(next.used, length, value, pos, length);
            object.extents.push_back({next.tail, next.used, length});
            next.used += length;
            pos += length;
            stripe.version = StripeId::next(); // Copy-on-write preserves the previously committed stripe.
            auto blocks = codec_.encode(data);
            write_shards(stripe, blocks, indices);
            next.stripes[next.tail] = std::move(stripe);
        }
        next.keys[id] = std::move(object);
        commit(std::move(next));
        record_write(update);
    }
    std::string get(const std::string& key) {
        const auto& object = lookup(key);
        std::string value;
        value.reserve(object.size());
        for (const auto& extent : object.extents) {
            if (catalog_.buffers.count(extent.stripe)) {
                value += buffer_data(catalog_.buffers.at(extent.stripe)).substr(extent.offset, extent.length);
                continue;
            }
            const auto& stripe = catalog_.stripes.at(extent.stripe);
            size_t position = extent_offset(catalog_, extent), remaining = extent.length;
            while (remaining) {
                int index = static_cast<int>(position / stripe.block_bytes);
                size_t offset = position % stripe.block_bytes;
                size_t length = std::min(remaining, static_cast<size_t>(stripe.block_bytes) - offset);
                std::string block;
                try {
                    block = read_shard_range(stripe, index, offset, length);
                } catch (const std::exception&) { /* Recover only when the requested data shard is unavailable. */ }
                if (block.empty()) {
                    block = recover_shard_range(stripe, index, offset, length);
                }
                value += block;
                position += length;
                remaining -= length;
            }
        }
        if (value.size() != object.size()) throw Error("UNAVAILABLE", "value length mismatch");
        ++gets_;
        return value;
    }
    uint64_t repair(const std::set<std::string>& ids, int target = -1, bool force = false) {
        uint64_t repaired = 0;
        for (const auto& id : ids) {
            if (catalog_.buffers.count(id)) {
                if (force) {
                    const auto& buffer = catalog_.buffers.at(id);
                    require(std::find(buffer.replicas.begin(), buffer.replicas.end(), target) != buffer.replicas.end(),
                            "repair target is not a buffer replica");
                    node_rpc(target, {"BUFFER_WRITE", buffer.version, buffer_data(buffer)});
                    ++repaired; ++repairs_; continue;
                }
                auto count = repair_buffer(catalog_.buffers.at(id), target);
                repaired += count; repairs_ += count;
                continue;
            }
            const auto& stripe = catalog_.stripes.at(id);
            std::vector<int> missing;
            PendingReplies checks(width(stripe));
            std::vector<int> checked;
            for (int i = 0; i < width(stripe); ++i) {
                if (target >= 0 && node_for(stripe, i) != target) continue;
                if (force) { missing.push_back(i); continue; }
                checked.push_back(i);
                checks.add(node_submit(node_for(stripe, i), {"CHECK_RANGES", pack_record({block_name(stripe, i),
                    std::to_string(stored_bytes(stripe, i)), "0", std::to_string(stored_bytes(stripe, i))})}));
            }
            size_t n = 0;
            for (auto& task : checks) {
                int i = checked[n++];
                try {
                    if (node_reply(node_for(stripe, i), task)[1] != "1") missing.push_back(i);
                } catch (const std::exception&) { missing.push_back(i); }
            }
            for (int i : missing) {
                // Restore the physical codeword, including InPlace's retained
                // nonzero tail. GET alone only needs the logical object prefix.
                const auto length = stored_bytes(stripe, i);
                std::string block; block.reserve(length);
                for (size_t pos = 0; pos < length;) {
                    auto count = std::min<size_t>(MAX_VALUE, length - pos);
                    block += recover_shard_range(stripe, i, pos, count);
                    pos += count;
                }
                node_rpc(node_for(stripe, i), {"WRITE", block_name(stripe, i), std::move(block)});
                ++repaired; ++repairs_;
            }
        }
        return repaired;
    }
    std::string metadata_json(const Object& object) const {
        std::ostringstream out;
        out << "{\"size\":" << object.size() << ",\"extents\":[";
        for (size_t e = 0; e < object.extents.size(); ++e) {
            const auto& extent = object.extents[e];
            bool buffered = catalog_.buffers.count(extent.stripe);
            size_t block_bytes = buffered ? catalog_.buffers.at(extent.stripe).bytes : catalog_.stripes.at(extent.stripe).block_bytes;
            const auto offset = extent_offset(catalog_, extent);
            out << (e ? "," : "") << "{\"stripe\":\"" << extent.stripe << "\",\"storage\":\"" << (buffered ? "buffer" : "ec")
                << "\",\"offset\":" << offset << ",\"length\":" << extent.length
                << ",\"block_index\":" << offset / block_bytes << ",\"block_offset\":" << offset % block_bytes
                << ",\"block_bytes\":" << block_bytes;
            if (buffered) {
                const auto& buffer = catalog_.buffers.at(extent.stripe);
                out << ",\"data_lengths\":[" << buffer.used << "],\"data_blocks\":1,\"parity_blocks\":0,\"shards\":[],\"replicas\":[";
                for (size_t i = 0; i < buffer.replicas.size(); ++i)
                    out << (i ? "," : "") << "{\"node\":" << buffer.replicas[i] << ",\"block\":\"" << buffer.version << "\"}";
                out << "]}";
            }
            else {
                const auto& stripe = catalog_.stripes.at(extent.stripe);
                out << ",\"data_lengths\":[";
                for (size_t i = 0; i < stripe.lengths.size(); ++i) out << (i ? "," : "") << stripe.lengths[i];
                out << "],\"data_blocks\":" << stripe.data_blocks << ",\"parity_blocks\":" << m_ << ",\"shards\":[";
                for (int i = 0; i < width(stripe); ++i)
                    out << (i ? "," : "") << "{\"index\":" << i << ",\"node\":" << node_for(stripe, i)
                        << ",\"role\":\"" << (i < stripe.data_blocks ? "data" : "parity") << "\",\"block\":\""
                        << block_name(stripe, i) << "\",\"stored_bytes\":" << stored_bytes(stripe, i) << '}';
                out << "]}";
            }
        }
        out << "]}";
        return out.str();
    }

#include "gc_store.inc"

public:
    Store() : k_(integer(env("EC_K", "6"), 1, 31)), m_(integer(env("EC_M", "3"), 1, 31)),
        bytes_(integer(env("BLOCK_SIZE", "4096"), 64, 1024 * 1024)),
        timeout_(integer(env("NODE_TIMEOUT_MS", "1000"), 10, 60000)), codec_(k_, m_, bytes_), buffer_writers_(context_, m_, timeout_) {
        require(coding_unit_ == "object" || coding_unit_ == "block", "invalid CODING_UNIT");
        require(strategy_ == "rmw" || strategy_ == "foreground_regroup" || strategy_ == "fastgc" || strategy_ == "inplace", "invalid UPDATE_STRATEGY");
        require((strategy_ != "fastgc" && strategy_ != "inplace") || coding_unit_ == "object", "strategy requires object coding units");
        require(gc_stop_percent_ < gc_start_percent_ && gc_space_stop_ < gc_space_start_, "GC stop thresholds must be below start thresholds");
        nodes_ = load_nodes(env("NODES_FILE", "/app/nodes.conf"));
        require(nodes_.size() >= static_cast<size_t>(codec_.width()), "need at least K+M distinct nodes");
        node_requests_ = std::make_unique<RequestWorkers>(context_, nodes_);
        catalog_.object_layout = coding_unit_ == "object";
        StripeId::initialize(uuid());
        require(WithGc == (strategy_ == "fastgc"), "GC metadata specialization mismatch");
        gc_index_ = StripeAccounting(nodes_.size(), m_);
        if (strategy_ != "inplace") gc_thread_ = std::thread([this] { gc_loop(); });
    }
    ~Store() {
        { std::lock_guard<std::mutex> guard(mutex_); gc_stop_ = true; gc_cv_.notify_one(); }
        if (gc_thread_.joinable()) gc_thread_.join();
    }
    Frames handle(const Frames& request) {
        std::unique_lock<std::mutex> guard(mutex_);
        require(!request.empty(), "missing operation");
        const auto& op = request[0];
        if (op == "PING") { require(request.size() == 1, "PING frame count"); return {"OK", "ec-kv/10-memcached"}; }
        if (op == "PHASE") {
            require(request.size() == 2 && request[1] == "RESIZE", "PHASE requires RESIZE");
            require(!rmw_active_, "cannot change phase during an unfinished mutation");
            PendingReplies tasks(nodes_.size());
            for (size_t node = 0; node < nodes_.size(); ++node)
                tasks.add(node_submit(node, {"PHASE", "RESIZE"}));
            for (auto& task : tasks) {
                auto reply = task.get();
                if (reply.size() != 2 || reply[0] != "OK") throw Error("UNAVAILABLE", "node phase switch failed");
            }
            begin_resize_phase();
            gc_wake_ = true; gc_cv_.notify_one();
            return {"OK", "resize"};
        }
        if (op == "CHECK_METADATA") { require(request.size() == 1, "CHECK_METADATA frame count"); return {"OK", check_owner_references()}; }
        if (op == "METADATA_MEMORY") { require(request.size() == 1, "METADATA_MEMORY frame count"); return {"OK", metadata_memory_report()}; }
        if (op == "SNAPSHOT") {
            require(request.size() == 1, "SNAPSHOT frame count");
            return {"OK", pack(catalog_)}; // Read-only diagnostic serialization, never stored.
        }
        if (op == "GC") {
            require(request.size() == 2, "GC requires STATUS/RUN/PAUSE/RESUME/QUIESCE/RESET_SPACE_PEAKS");
            if (request[1] == "STATUS") return {"OK", gc_stats()};
            require(strategy_ == "fastgc", "GC control requires fastgc strategy");
            if (request[1] == "RESET_SPACE_PEAKS") {
                require(catalog_.object_layout, "space peaks require object layout");
                gc_space_peaks_ = {};
                gc_amplification_peak_ = {};
                observe_gc_space();
                return {"OK", gc_stats()};
            }
            if (request[1] == "QUIESCE") {
                // Count-triggered rounds already queued by committed updates
                // must finish too, including the final update's trigger.
                if (gc_every_updates_ && !gc_paused_ && !gc_suspended_)
                    gc_cv_.wait(guard, [&] { return !gc_update_pending_ && !gc_busy_; });
                // Stop scheduling; drain the entire in-flight round AND cleanup.
                // wait releases mutex_, so background completion cannot deadlock.
                gc_paused_ = true; gc_suspended_ = true; gc_forced_ = false; gc_wake_ = false;
                gc_cv_.wait(guard, [&] { return !gc_busy_; });
                return {"OK", gc_stats()};
            }
            if (request[1] == "RUN") { gc_suspended_ = false; gc_forced_ = true; }
            else if (request[1] == "PAUSE") gc_paused_ = true;
            else if (request[1] == "RESUME") { gc_suspended_ = false; gc_paused_ = false; }
            else throw Error("INVALID", "GC requires STATUS/RUN/PAUSE/RESUME/QUIESCE/RESET_SPACE_PEAKS");
            gc_wake_ = true; gc_cv_.notify_one();
            return {"OK", gc_stats()};
        }
        if (op == "STATS") {
            require(request.size() == 1, "STATS frame count");
            std::ostringstream out;
            out << "{\"storage_mode\":\"memory\",\"storage_backend\":\"" << storage_backend() << "\",\"k\":" << k_ << ",\"m\":" << m_ << ",\"block_size\":" << bytes_
                << ",\"workload_phase\":\"" << workload_phase()
                << "\",\"coding_unit\":\"" << coding_unit_ << "\",\"catalog_layout\":\"" << (catalog_.object_layout ? "object" : "block")
                << "\",\"migration_pending\":" << (coding_unit_ == "object" && !catalog_.object_layout ? "true" : "false")
                << ",\"keys\":" << catalog_.keys.size() << ",\"stripes\":" << catalog_.stripes.size()
                << ",\"tail_used_bytes\":" << catalog_.used << ",\"stripe_capacity\":" << (catalog_.object_layout ? "null" : std::to_string(capacity()))
                << ",\"puts\":" << puts_ << ",\"gets\":" << gets_ << ",\"updates\":" << updates_
                << ",\"deletes\":" << deletes_ << ",\"repaired_shards\":" << repairs_
                << ",\"update_strategy\":\"" << strategy_ << "\",\"pending_update\":" << (rmw_active_ ? "true" : "false")
                << ",\"foreground_metadata_mode\":\"" << (catalog_.object_layout ? "indexed_affected_records" : "legacy_full_catalog") << "\""
                << ",\"metadata\":{\"object_record_bytes\":" << sizeof(Object)
                << ",\"extent_record_bytes\":" << sizeof(Extent) << ",\"stripe_id_bytes\":" << sizeof(StripeId)
                << ",\"stripe_record_bytes\":" << sizeof(Stripe) << ",\"length_record_bytes\":" << sizeof(uint32_t)
                << ",\"separate_gc_entry_count\":0,\"gc_candidate_entries\":" << gc_index_.dirty.size() + gc_index_.empty.size() << '}'
                << ",\"foreground_owner_visits\":" << foreground_owner_visits_
                << ",\"direct_rmw_updates\":" << direct_rmw_updates_
                << ",\"owner_index_mode\":\"" << (strategy_ == "inplace" ? "buffer_only" : "stripe_key_references")
                << "\",\"epoch_mode\":\"" << (WithGc ? "embedded_stripe" : "none") << "\",\"owner_index_entries\":"
                << (catalog_.object_layout ? (strategy_ == "inplace" ? catalog_.buffers.size() : catalog_.keys.size()) : 0)
                << ",\"foreground_metadata_keys_max\":" << foreground_keys_max_
                << ",\"foreground_metadata_stripes_max\":" << foreground_stripes_max_
                << ",\"update_failures\":" << update_failures_
                << ",\"resize_updates\":" << resizes_ << ",\"peeled_blocks\":" << peeled_
                << ",\"inplace_update_mode\":\"delta_rmw_retained_tail\",\"inplace_delta_updates\":" << inplace_delta_updates_
                << ",\"inplace_retained_tail_bytes\":" << (strategy_ == "inplace" ? gc_index_.total - gc_index_.invalid - gc_index_.logical : 0)
                << ",\"inplace_reencodes\":" << inplace_reencodes_ << ",\"inplace_buffer_updates\":" << inplace_buffer_updates_
                << ",\"reencoded_remainders\":" << reencoded_ << ",\"regrouped_stripes\":" << regrouped_
                << ",\"buffer_blocks\":" << catalog_.buffers.size()
                << ",\"gc\":" << gc_stats()
                << ",\"buffer_replication\":" << buffer_writers_.stats()
                << ",\"node_requests\":" << node_requests_->stats()
                << ",\"maintenance_traffic\":{\"unit\":\"rpc_application_frame_bytes\",\"kvmd_regroup\":"
                << regroup_traffic_.stats(nodes_.size()) << ",\"fastgc_gc\":" << gc_traffic_.stats(nodes_.size()) << '}' 
                << ",\"temporary_replica_cleanup\":" << (replica_cleanup_ ? "true" : "false")
                << ",\"buffer_delete_requests\":" << buffer_delete_requests_
                << ",\"buffer_cleanup_mode\":\"node_batch\",\"buffer_delete_items\":" << buffer_delete_items_
                << ",\"buffer_cleanup_rounds\":" << buffer_cleanup_rounds_
                << ",\"retired_buffers\":" << catalog_.retired_buffers.size() << ",\"stripe_widths\":{";
            std::map<int, size_t> widths;
            for (const auto& [id, stripe] : catalog_.stripes) ++widths[stripe.data_blocks];
            bool first = true;
            for (const auto& [count, number] : widths) {
                out << (first ? "" : ",") << '\"' << count << "\":" << number;
                first = false;
            }
            out << "},\"nodes\":[";
            std::vector<std::future<Frames>> tasks;
            for (size_t n = 0; n < nodes_.size(); ++n) tasks.push_back(std::async(std::launch::async, [&, n] {
                try { return Frames{node_rpc(n, {"STATS"}, stats_timeout_)[1], ""}; }
                catch (const std::exception& error) { return Frames{"", error.what()}; }
                catch (...) { return Frames{"", "unknown node STATS exception"}; }
            }));
            bool complete = true;
            for (size_t n = 0; n < nodes_.size(); ++n) {
                auto result = tasks[n].get();
                const auto& stats = result[0];
                complete &= !stats.empty();
                out << (n ? "," : "") << "{\"id\":" << n << ",\"endpoint\":\"" << nodes_[n]
                    << "\",\"healthy\":" << (stats.empty() ? "false" : "true")
                    << ",\"parity\":" << (stats.empty() ? "null" : stats)
                    << ",\"stats_timeout_ms\":" << stats_timeout_
                    << ",\"error\":" << (stats.empty() ? json_string(result[1].empty() ? "empty node STATS reply" : result[1]) : "null") << '}';
            }
            out << "],\"stats_complete\":" << (complete ? "true" : "false") << '}';
            return {"OK", out.str()};
        }
        // No operation may observe mixed data/parity while an interrupted update is pending.
        migrate_objects();
        ensure_buffer_replicas();
        cleanup_buffers();
        if (op == "PUT" || op == "UPDATE") {
            require(request.size() == 3, "PUT/UPDATE requires key and value");
            try {
                cleanup_kvmd_stripes(); // Retry cleanup left by a transport failure.
                put(request[1], request[2], op == "UPDATE");
                cleanup_kvmd_stripes(); // Include old EC deletion in foreground latency.
                cleanup_buffers();
            } catch (...) {
                ++update_failures_;
                throw;
            }
            return {"OK", ""};
        }
        if (op == "REPAIR_BENCH") {
            require(request.size() == 3, "REPAIR_BENCH key target_node");
            int node = integer(request[2], 0, nodes_.size() - 1);
            std::set<std::string> ids;
            for (const auto& extent : lookup(request[1]).extents) {
                auto buffer = catalog_.buffers.find(extent.stripe);
                if (buffer == catalog_.buffers.end()) {
                    const auto& stripe = catalog_.stripes.at(extent.stripe);
                    int index = extent_offset(catalog_, extent) / stripe.block_bytes;
                    require(node_for(stripe, index) == node, "repair target is not object data node");
                }
                ids.insert(extent.stripe);
            }
            return {"OK", std::to_string(repair(ids, node, true))};
        }
        require(request.size() == 2, "operation requires one argument");
        if (op == "GET") return {"OK", get(request[1])};
        if (op == "META") return {"OK", metadata_json(lookup(request[1]))};
        if (op == "DELETE") {
            auto id = key_id(request[1]);
            if (!catalog_.keys.count(id)) throw Error("NOT_FOUND", "key not found");
            if (catalog_.object_layout) {
                auto edit = object_edit(id, false, false);
                const auto source = edit.next.keys.at(id).extents.at(0).stripe;
                if (edit.next.buffers.count(source)) erase_buffer(edit.next, source);
                edit.next.keys.erase(id);
                commit_object(std::move(edit));
            } else {
                Catalog next = catalog_;
                next.keys.erase(id);
                commit(std::move(next));
            }
            ++deletes_;
            return {"OK", ""};
        }
        if (op == "REPAIR") {
            std::set<std::string> ids;
            for (const auto& extent : lookup(request[1]).extents) ids.insert(extent.stripe);
            return {"OK", std::to_string(repair(ids))};
        }
        if (op == "REPAIR_NODE") {
            int node = integer(request[1], 0, nodes_.size() - 1);
            std::set<std::string> ids;
            for (const auto& [id, stripe] : catalog_.stripes)
                for (int i = 0; i < width(stripe); ++i) if (node_for(stripe, i) == node) ids.insert(id);
            for (const auto& [id, buffer] : catalog_.buffers)
                if (std::find(buffer.replicas.begin(), buffer.replicas.end(), node) != buffer.replicas.end()) ids.insert(id);
            return {"OK", std::to_string(repair(ids, node))};
        }
        throw Error("INVALID", "unknown operation");
    }
};
int main() {
    try {
        std::unique_ptr<Store<false>> store;
        std::unique_ptr<Store<true>> fastgc;
        std::unique_ptr<MosaicStore> mosaic;
        if (env("UPDATE_STRATEGY", "mosaic") == "mosaic") mosaic = std::make_unique<MosaicStore>();
        else if (env("UPDATE_STRATEGY", "mosaic") == "fastgc") fastgc = std::make_unique<Store<true>>();
        else store = std::make_unique<Store<false>>();
        Context context;
        const auto endpoint = env("BIND", "tcp://*:5555");
        std::cout << "ec-kv proxy bind=" << endpoint << std::endl;
        // Validate once before readiness/first operation, never scan on updates.
        // Configuration applies to the entire experiment, including all schemes.
        std::once_flag backend_verified;
        const auto nodes = load_nodes(env("NODES_FILE", "/app/nodes.conf"));
        auto handler = [&](const Frames& request) {
            std::call_once(backend_verified, [&] {
                for (size_t n = 0; n < nodes.size(); ++n) {
                    auto reply = rpc(context, nodes[n], {"BACKEND", std::to_string(n)}, 1000);
                    require(reply.size() == 2 && reply[0] == "OK" && reply[1] == storage_backend(),
                            "storage backend mismatch at node " + std::to_string(n));
                }
            });
            return mosaic ? mosaic->handle(request) : fastgc ? fastgc->handle(request) : store->handle(request);
        };
        if (mosaic) serve_concurrent(context, endpoint, integer(env("PROXY_REQUEST_THREADS", "8"), 2, 64),
            handler, [](const Frames&) { return false; });
        else { Socket server(context, ZMQ_REP, -1); server.bind(endpoint); serve(server, handler); }
    } catch (const std::exception& e) { std::cerr << e.what() << std::endl; return 1; }
}
