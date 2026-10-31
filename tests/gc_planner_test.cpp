#include "gc_planner.h"
#include <cassert>
#include <iostream>
#include <set>
using namespace ec;
GcCandidate candidate(std::string id, int count, int node, int length, uint64_t physical) {
    GcCandidate c{id, (uint32_t(1) << count) - 1, physical, {}};
    for (int i = 0; i < count; ++i) c.objects.push_back({id + std::to_string(i), id, i, node + i, static_cast<size_t>(length)});
    return c;
}
int main() {
    auto a = candidate("a", 2, 0, 100, 500), b = candidate("b", 2, 2, 100, 500), c = candidate("c", 2, 0, 100, 500);
    assert(plan_gc({a, b}, 3, 2, 5).groups.empty()); // 4 objects cannot fill a 3-object output.
    auto p = plan_gc({a, b, c}, 3, 2, 5);
    assert(p.sources.size() == 3 && p.groups.size() == 2 && p.old_bytes - p.new_bytes == 500);
    std::set<std::string> keys;
    for (const auto& group : p.groups) {
        assert(group.objects.size() == 3 && group.bytes == 100);
        for (const auto& object : group.objects) assert(keys.insert(object.key).second);
    }
    assert(keys.size() == 6);
    // Placement avoids a data transfer when the original nodes fit one rotation.
    p = plan_gc({candidate("a", 2, 0, 100, 600), candidate("b", 2, 2, 100, 600)}, 4, 2, 6);
    assert(p.groups.size() == 1 && p.remote_data_bytes == 0 && p.groups[0].start == 0);
    // Short objects no longer acquire stored padding from a long peer.
    p = plan_gc({candidate("a", 1, 0, 10000, 20000), candidate("b", 3, 1, 1, 4)}, 4, 1, 5);
    assert(p.groups.size() == 1 && p.new_bytes == 20003 && p.old_bytes == 20004);
    // Empty values still count as independent objects.
    p = plan_gc({candidate("a", 1, 0, 0, 320), candidate("b", 2, 1, 0, 320)}, 3, 2, 5);
    assert(p.groups.size() == 1 && p.groups[0].bytes == 64 && p.new_bytes == 0);
    std::cout << "PASS GC planner: multi-output merges, distinct objects, node locality, padding cost and empty values\n";
}
