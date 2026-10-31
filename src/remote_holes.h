#pragma once
#include "node_holes.h"
#include <functional>

namespace ec::mosaic {
// The proxy holds only scalar capacity/measurement reports and a transient
// allocation plan. It never owns a hole tree or searches individual holes.
class RemoteHoles {
    std::function<Frames(Frames)> transport_;
    std::string owner_, coefficient_;
    uint64_t sequence_ = 0;
    Frames pending_;
    size_t fmax_ = 0, capacity_ = 0, free_ = 0, holes_ = 0, chunks_ = 0;
    uint64_t metadata_ = 0, useful_ = 0, rb_ = 0;
    uint64_t revision_ = 0;
    Frames exchange() {
        try { return transport_(pending_); }
        catch (const Error& e) {
            if (e.status != "UNAVAILABLE") pending_.clear();
            throw;
        }
    }
    std::string call(const std::string& op, const std::string& body) {
        if (!pending_.empty()) {
            auto previous = pending_;
            if ((previous[4] != op || previous[5] != body) && !(previous[4] == "ADD" && op == "REMOVE"))
                throw Error("UNAVAILABLE", "hole group has an unresolved operation; retry its owner first");
            auto reply = exchange(); // Same sequence after transport loss.
            pending_.clear();
            if (previous[4] == op && previous[5] == body) return reply[1];
        }
        require(sequence_ != UINT64_MAX, "allocator sequence exhausted");
        pending_ = {"HOLES", owner_, coefficient_, std::to_string(sequence_ + 1), op, body};
        ++sequence_;
        auto reply = exchange();
        pending_.clear();
        return reply[1];
    }
public:
    void bind(std::string owner, int coefficient, size_t fmax, std::function<Frames(Frames)> transport) {
        owner_ = std::move(owner); coefficient_ = std::to_string(coefficient);
        fmax_ = fmax; transport_ = std::move(transport);
    }
    void report(std::istream& in) {
        uint64_t revision, capacity, free, holes, chunks, metadata, useful, rb;
        in >> revision >> capacity >> free >> holes >> chunks >> metadata >> useful >> rb;
        require(bool(in), "invalid node hole capacity report");
        if (revision >= revision_) {
            revision_ = revision; capacity_ = capacity; free_ = free; holes_ = holes;
            chunks_ = chunks; metadata_ = metadata; useful_ = useful; rb_ = rb;
        }
    }
    std::optional<Plan> allocate(size_t length, size_t fmax, size_t tau) {
        if (pending_.empty() && fmax <= fmax_ && length > capacity_) return std::nullopt;
        auto raw = call("PLAN", std::to_string(length)+" "+std::to_string(fmax)+" "+std::to_string(tau));
        if (raw.empty()) return std::nullopt;
        return decode_plan(raw);
    }
    bool commit(const Plan& p) { return call("COMMIT", encode_plan(p)) == "1"; }
    void add_chunk(uint64_t id, size_t size) { call("ADD", std::to_string(id)+" "+std::to_string(size)); }
    bool remove_empty_chunk(uint64_t id) { return call("REMOVE", std::to_string(id)) == "1"; }
    void release(const Hole& h) { call("RELEASE", std::to_string(h.chunk)+" "+std::to_string(h.offset)+" "+std::to_string(h.length)); }
    size_t free_bytes() const { return free_; }
    size_t hole_count() const { return holes_; }
    size_t chunk_count() const { return chunks_; }
    uint64_t metadata_bytes() const { return metadata_; }
    uint64_t useful_metadata_bytes() const { return useful_; }
    uint64_t rb_tree_metadata_bytes() const { return rb_; }
};
} // namespace ec::mosaic
