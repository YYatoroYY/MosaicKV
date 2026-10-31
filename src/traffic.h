#pragma once
#include "common.h"
#include <array>
#include <atomic>

namespace ec {
// Explicit one-way experiment boundary. Atomic because sender threads consult
// this flag; each queued job captures whether its own RPC is counted.
inline std::atomic<bool>& resize_phase_flag() {
    static std::atomic<bool> active{env("EC_WORKLOAD_PHASE", "resize") == "resize"};
    return active;
}
inline bool resize_phase() { return resize_phase_flag().load(std::memory_order_relaxed); }
inline void begin_resize_phase() { resize_phase_flag().store(true, std::memory_order_relaxed); }
inline const char* workload_phase() { return resize_phase() ? "resize" : "load"; }
// EC_TRAFFIC=0 still suppresses measurement during the resize phase.
inline bool traffic_enabled() {
    static const bool enabled = env("EC_TRAFFIC", "1") == "1";
    return enabled && resize_phase();
}
inline uint64_t frame_bytes(const Frames& frames) {
    uint64_t bytes = 0;
    for (const auto& frame : frames) bytes += frame.size();
    return bytes;
}
// Application multipart frame bytes, excluding diagnostics and transport framing.
inline bool traffic_operation(const Frames& request) {
    if (request.empty()) return false;
    const auto& op = request[0] == "MOSAIC" && request.size() == 5 ? request[2] : request[0];
    return op != "STATS" && op != "MEMORY" && op != "PROFILE" && op != "PING" && op != "CONFIG" && op != "BACKEND" && op != "LAYOUT" && op != "PHASE";
}

// Sender-side RPC accounting, for links and labelled maintenance subsets.
// Includes completed error replies and retries. Link totals count each RPC once.
struct RpcTraffic {
    std::atomic<uint64_t> requests{0}, replies{0}, request_bytes{0}, reply_bytes{0};
    void sent(const Frames& frames) {
        if (!traffic_operation(frames)) return;
        sent(frame_bytes(frames));
    }
    void sent(uint64_t bytes) {
        request_bytes.fetch_add(bytes, std::memory_order_relaxed);
        requests.fetch_add(1, std::memory_order_relaxed);
    }
    void received(const Frames& frames) {
        received(frame_bytes(frames));
    }
    void received(uint64_t bytes) {
        reply_bytes.fetch_add(bytes, std::memory_order_relaxed);
        replies.fetch_add(1, std::memory_order_relaxed);
    }
};
class RpcTrafficGroup {
    std::array<RpcTraffic, 256> nodes_;
public:
    RpcTraffic* node(size_t index) { return &nodes_.at(index); }
    // RX/TX are named from the destination's perspective: request/reply bytes.
    std::string stats(size_t count) const {
        std::ostringstream out;
        out << '[';
        for (size_t i = 0; i < count; ++i) {
            const auto& n = nodes_.at(i);
            auto rx = n.request_bytes.load(std::memory_order_relaxed);
            auto tx = n.reply_bytes.load(std::memory_order_relaxed);
            out << (i ? "," : "") << "{\"node\":" << i
                << ",\"received_bytes\":" << rx << ",\"sent_bytes\":" << tx
                << ",\"total_bytes\":" << rx + tx << ",\"requests\":" << n.requests.load()
                << ",\"replies\":" << n.replies.load() << '}';
        }
        out << ']';
        return out.str();
    }
};
} // namespace ec
