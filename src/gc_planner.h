#pragma once
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace ec {
struct GcObject {
    std::string key, source;
    int index = 0, node = 0;
    size_t length = 0;
};
struct GcCandidate {
    std::string id;
    uint32_t live_mask = 0;
    uint64_t physical_bytes = 0;
    std::vector<GcObject> objects;
};
struct GcGroup {
    int start = 0, bytes = 64;
    std::vector<GcObject> objects; // Data coefficient order, one object per slot.
};
struct GcPlan {
    std::vector<std::string> sources;
    std::vector<GcGroup> groups;
    uint64_t old_bytes = 0, new_bytes = 0, remote_data_bytes = 0;
};
// Simplified FastGC-inspired heuristic, not the paper's grouping/max-flow algorithm.
// Whole source stripes are selected, so every committed merge can release its sources.
// Bounded knapsack considers multiple output stripes and counts empty objects via bits.
inline GcPlan plan_gc(const std::vector<GcCandidate>& candidates, int k, int m, int nodes) {
    struct Choice { bool valid = false; uint64_t bytes = 0; std::vector<size_t> selected; };
    std::vector<Choice> dp(candidates.size() * k + 1);
    dp[0].valid = true;
    GcPlan best;
    for (size_t c = 0; c < candidates.size(); ++c) {
        auto count = static_cast<size_t>(__builtin_popcount(candidates[c].live_mask));
        if (!count) continue;
        for (size_t n = dp.size(); n-- > count;) {
            if (!dp[n - count].valid) continue;
            uint64_t bytes = dp[n - count].bytes + candidates[c].physical_bytes;
            if (dp[n].valid && dp[n].bytes >= bytes) continue;
            dp[n] = dp[n - count];
            dp[n].bytes = bytes;
            dp[n].selected.push_back(c);
        }
        // Evaluate prefixes too: a later large object must not hide a useful smaller merge.
        for (size_t n = k; n < dp.size(); n += k) {
            if (!dp[n].valid) continue;
            GcPlan plan;
            std::vector<GcObject> objects;
            for (auto index : dp[n].selected) {
                const auto& source = candidates[index];
                plan.sources.push_back(source.id);
                plan.old_bytes += source.physical_bytes;
                objects.insert(objects.end(), source.objects.begin(), source.objects.end());
            }
            std::stable_sort(objects.begin(), objects.end(), [](const auto& a, const auto& b) { return a.length > b.length; });
            for (size_t offset = 0; offset < objects.size(); offset += k) {
                GcGroup group;
                group.bytes = std::max<size_t>(64, objects[offset].length);
                group.objects.assign(objects.begin() + offset, objects.begin() + offset + k);
                plan.new_bytes += static_cast<uint64_t>(m) * objects[offset].length;
                for (const auto& object : group.objects) plan.new_bytes += object.length;
                plan.groups.push_back(std::move(group));
            }
            if (plan.new_bytes >= plan.old_bytes) continue;
            auto gain = plan.old_bytes - plan.new_bytes;
            auto best_gain = best.old_bytes - best.new_bytes;
            if (gain < best_gain) continue;
            for (auto& group : plan.groups) {
                int most_local = -1;
                // Try every rotation; keep as many source data nodes as possible. A node
                // receives only one shard of a stripe, even if sources overlap there.
                for (int start = 0; start < nodes; ++start) {
                    uint32_t occupied = 0;
                    std::vector<GcObject> ordered(k), moved;
                    for (int i = 0; i < k; ++i) {
                        const auto& object = group.objects[i];
                        int slot = (object.node - start + nodes) % nodes;
                        if (slot < k && !(occupied & (uint32_t(1) << slot))) {
                            occupied |= uint32_t(1) << slot;
                            ordered[slot] = object;
                        } else moved.push_back(object);
                    }
                    int local = __builtin_popcount(occupied);
                    if (local <= most_local) continue;
                    most_local = local;
                    size_t next = 0;
                    for (int i = 0; i < k; ++i)
                        if (!(occupied & (uint32_t(1) << i))) ordered[i] = moved[next++];
                    group.start = start;
                    group.objects = std::move(ordered);
                }
                for (int i = 0; i < k; ++i)
                    if (group.objects[i].node != (group.start + i) % nodes) plan.remote_data_bytes += group.objects[i].length;
            }
            if (gain > best_gain || (gain == best_gain && plan.remote_data_bytes < best.remote_data_bytes)) best = std::move(plan);
        }
    }
    return best;
}
} // namespace ec
