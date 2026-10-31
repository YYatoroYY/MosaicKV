#pragma once
#include "common.h"

namespace ec {
// REP-thread-owned scratch, never shared with queued RPCs. A scope returns it
// even on exceptions. Bound retained capacity across all strings, so a large
// request cannot permanently inflate every node's working set.
class FrameScratch {
    Frames frames_;
    size_t limit_;
    void trim() noexcept {
        size_t retained = 0;
        for (auto& frame : frames_) {
            if (frame.capacity() > limit_ - retained) std::string().swap(frame);
            else { retained += frame.capacity(); frame.clear(); }
        }
    }
public:
    explicit FrameScratch(size_t limit) : limit_(limit) {}
    class Scope {
        FrameScratch& owner_;
    public:
        explicit Scope(FrameScratch& owner) : owner_(owner) {}
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;
        ~Scope() { owner_.trim(); }
        Frames& frames() { return owner_.frames_; }
    };
};
} // namespace ec
