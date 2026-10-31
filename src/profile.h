#pragma once
#include "common.h"
#include <chrono>
#include <mutex>

namespace ec {
// Optional, RAM-only diagnostics. Per-request records have a synchronized journal.
// Durations are local monotonic intervals, never cross-machine timestamps.
class Profile {
    using Clock = std::chrono::steady_clock;
    struct Record {
        const char* kind;
        std::string id;
        uint64_t total = 0;
        std::vector<std::pair<const char*, uint64_t>> stages, details;
    };
    struct Journal { std::vector<Record> records; uint64_t dropped = 0; };
    static Journal& journal() { static Journal j; return j; }
    static std::mutex& journal_mutex() { static std::mutex m; return m; }
    Record record_;
    bool active_;
    Clock::time_point start_, last_;
public:
    static bool enabled() { static const bool value = env("EC_PROFILE", "0") == "1"; return value; }
    static uint64_t now() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
    }
    Profile(const char* kind, bool selected = true) : record_{kind, "", 0, {}, {}}, active_(selected && enabled()) {
        if (active_) { start_ = last_ = Clock::now(); record_.stages.reserve(16); record_.details.reserve(4); }
    }
    bool active() const { return active_; }
    void id(const std::string& value) { if (active_) record_.id = value; }
    void mark(const char* stage) {
        if (!active_) return;
        auto current = Clock::now();
        record_.stages.emplace_back(stage, std::chrono::duration_cast<std::chrono::nanoseconds>(current - last_).count());
        last_ = current;
    }
    void detail(const char* key, uint64_t value) {
        if (!active_) return;
        for (auto& item : record_.details) if (std::strcmp(item.first, key) == 0) { item.second += value; return; }
        record_.details.emplace_back(key, value);
    }
    ~Profile() noexcept {
        if (!active_) return;
        try {
            mark("tail");
            record_.total = std::chrono::duration_cast<std::chrono::nanoseconds>(last_ - start_).count();
            std::lock_guard<std::mutex> lock(journal_mutex());
            auto& j = journal();
            if (j.records.size() < 100000) j.records.push_back(std::move(record_));
            else ++j.dropped;
        } catch (...) { std::lock_guard<std::mutex> lock(journal_mutex()); ++journal().dropped; }
    }
    Profile(const Profile&) = delete;
    Profile& operator=(const Profile&) = delete;
    static std::string dump(bool reset) {
        std::lock_guard<std::mutex> lock(journal_mutex());
        auto& j = journal();
        if (reset) { j.records.clear(); j.records.reserve(8192); j.dropped = 0; return ""; }
        std::ostringstream out;
        out << "{\"enabled\":" << (enabled() ? "true" : "false") << ",\"dropped\":" << j.dropped << ",\"records\":[";
        for (size_t i = 0; i < j.records.size(); ++i) {
            const auto& r = j.records[i];
            // IDs are internal hexadecimal transaction identifiers, optionally suffixed _N.
            out << (i ? "," : "") << "{\"kind\":\"" << r.kind << "\",\"id\":\"" << r.id << "\",\"total_ns\":" << r.total;
            auto fields = [&](const char* name, const auto& values) {
                out << ",\"" << name << "\":{";
                for (size_t n = 0; n < values.size(); ++n)
                    out << (n ? "," : "") << '"' << values[n].first << "\":" << values[n].second;
                out << '}';
            };
            fields("stages", r.stages); fields("details", r.details);
            out << '}';
        }
        out << "]}";
        return out.str();
    }
};
} // namespace ec
