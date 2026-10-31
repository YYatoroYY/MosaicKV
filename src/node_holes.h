#pragma once
#include "common.h"
#include "mosaic_allocator.h"
#include "record_view.h"
#include <mutex>

namespace ec::mosaic {
inline std::string encode_plan(const Plan& plan) {
    std::ostringstream out;
    out << plan.revision << ' ' << plan.length;
    for (const auto& p : plan.pieces) out << ' ' << p.hole.chunk << ' ' << p.hole.offset << ' ' << p.hole.length << ' ' << p.take;
    return out.str();
}
inline Plan decode_plan(const std::string& raw) {
    std::istringstream in(raw);
    Plan plan{}; in >> plan.revision >> plan.length;
    require(bool(in), "invalid allocation plan");
    uint64_t chunk; size_t offset, length, take;
    while (in >> chunk) {
        in >> offset >> length >> take;
        require(bool(in) && plan.pieces.size() < 4096, "invalid allocation pieces");
        plan.pieces.push_back({{chunk, offset, length}, take});
    }
    require(in.eof(), "trailing allocation plan");
    return plan;
}

// Memory-node-owned length/address red-black trees. A sequence number makes
// Allocate/Release retries safe after a lost acknowledgement. Only the latest
// reply per coefficient is retained; earlier requests can never execute again.
class NodeHoles {
    struct Group { HoleTree tree; uint64_t sequence = 0; Frames request, reply; };
    std::vector<Group> groups_;
    std::string owner_;
    mutable std::mutex mutex_;
public:
    explicit NodeHoles(size_t k) : groups_(k) {}
    std::string report(size_t fmax) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostringstream out;
        for (const auto& g : groups_) {
            const auto& t = g.tree;
            out << t.revision() << ' ' << t.capacity(fmax) << ' ' << t.free_bytes() << ' ' << t.hole_count() << ' '
                << t.chunk_count() << ' ' << t.metadata_bytes() << ' ' << t.useful_metadata_bytes() << ' '
                << t.rb_tree_metadata_bytes() << '\n';
        }
        return out.str();
    }
    Frames handle(const Frames& r) {
        require(r.size() == 7, "HOLES frame count");
        const auto c = integer(r[3], 0, groups_.size()-1);
        const auto seq = std::stoull(r[4]);
        std::lock_guard<std::mutex> lock(mutex_);
        require(!r[2].empty() && (owner_.empty() || owner_ == r[2]), "hole index belongs to another proxy session");
        owner_ = r[2];
        auto& g = groups_[c];
        if (seq == g.sequence && seq) {
            require(r == g.request, "allocator sequence reused with different request");
            return g.reply;
        }
        require(seq == g.sequence + 1, "stale/out-of-order allocator request");
        // Allocate reply storage before a mutation. All responses are prepared
        // before committing tree changes (except read-only PLAN).
        Frames saved = r, reply{"OK", ""};
        const auto& op = r[5];
        std::istringstream in(r[6]);
        try {
        if (op == "PLAN") {
            size_t length, fmax, tau; in >> length >> fmax >> tau;
            require(bool(in) && fmax > 0 && fmax <= 4096, "invalid allocation parameters");
            auto plan = g.tree.allocate(length, fmax, tau);
            if (plan) reply[1] = encode_plan(*plan);
        } else if (op == "COMMIT") {
            auto plan = decode_plan(r[6]);
            reply[1] = "1";
            if (!g.tree.commit(plan)) reply[1] = "0";
        } else if (op == "ADD") {
            uint64_t id; size_t size; in >> id >> size;
            require(bool(in), "invalid chunk registration"); g.tree.add_chunk(id, size);
        } else if (op == "REMOVE") {
            uint64_t id; in >> id; require(bool(in), "invalid chunk removal");
            reply[1] = "1"; if (!g.tree.remove_empty_chunk(id)) reply[1] = "0";
        } else if (op == "RELEASE") {
            uint64_t id; size_t offset, length; in >> id >> offset >> length;
            require(bool(in), "invalid release"); g.tree.release({id, offset, length});
        } else throw Error("INVALID", "unknown hole operation");
        } catch (const Error& e) { reply = {e.status, e.what()}; }
        catch (const std::exception& e) { reply = {"INVALID", e.what()}; }
        g.request.swap(saved); g.reply.swap(reply); g.sequence = seq;
        return g.reply;
    }
};
} // namespace ec::mosaic
