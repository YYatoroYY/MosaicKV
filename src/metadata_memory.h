#pragma once
#include <malloc.h>
#include <cstdint>
#include <map>
#include <string>
#include <sstream>
#include <vector>

namespace ec::metadata_memory {
// Explicit, read-only diagnostic for this project's Linux/glibc/libstdc++ build.
// Counts allocator-usable persistent heap, including allocation rounding, but
// excluding malloc headers, container roots, payload, workers and EC workspaces.
// Never invoked by STATS or the update hot path.
inline uint64_t allocation(const void* p) {
    return p ? malloc_usable_size(const_cast<void*>(p)) : 0;
}
inline uint64_t string(const std::string& s) {
    auto p = reinterpret_cast<uintptr_t>(s.data()), begin = reinterpret_cast<uintptr_t>(&s);
    return p >= begin && p < begin + sizeof(s) ? 0 : allocation(s.data());
}
template<class T> uint64_t vector(const std::vector<T>& v) { return allocation(v.data()); }
template<class Tree> uint64_t nodes(const Tree& tree) {
    uint64_t bytes = 0;
    for (auto it = tree.begin(); it != tree.end(); ++it) bytes += allocation(it._M_node);
    return bytes;
}
// Logical fields of occupied libstdc++ RB nodes, excluding alignment padding.
// The enum color is a separate field here; intrusive hooks pack it in a pointer.
template<class Tree> uint64_t rb_tree_fields(const Tree& tree) {
    return tree.size() * (3 * sizeof(void*) + sizeof(std::_Rb_tree_color));
}
inline std::string with_rb_tree(std::string record, const std::map<std::string, uint64_t>& groups);
inline std::string report(const std::map<std::string, uint64_t>& groups, size_t keys) {
    uint64_t total = 0;
    for (const auto& item : groups) total += item.second;
    std::ostringstream out;
    out << "{\"scope\":\"persistent_catalog_heap_usable_bytes\",\"keys\":" << keys
        << ",\"total_bytes\":" << total << ",\"bytes_per_key\":" << (keys ? double(total) / keys : 0)
        << ",\"groups\":{";
    bool first = true;
    for (const auto& item : groups) { out << (first ? "" : ",") << '"' << item.first << "\":" << item.second; first = false; }
    out << "}}";
    return out.str();
}
// Separate structural census; a breakdown of existing storage, never added to
// useful application metadata or allocated totals.
inline std::string with_rb_tree(std::string record, const std::map<std::string, uint64_t>& groups) {
    auto fields = report(groups, 0);
    const std::string old_scope = "persistent_catalog_heap_usable_bytes";
    fields.replace(fields.find(old_scope), old_scope.size(), "occupied_rb_tree_logical_field_bytes_v1");
    for (const std::string field : {",\"keys\":0", ",\"bytes_per_key\":0"})
        fields.erase(fields.find(field), field.size());
    record.pop_back();
    return record + ",\"rb_tree\":" + fields + "}";
}
// Effective application fields in occupied records, not sizeof(struct),
// tree hooks, unused inline slots, vector capacity or allocator rounding.
// Keep the old allocation diagnostic unchanged and attach a versioned census.
inline std::string with_useful(std::string allocated, const std::map<std::string, uint64_t>& groups,
                               size_t keys, uint64_t key_bytes) {
    auto useful = report(groups, keys);
    const std::string old_scope = "persistent_catalog_heap_usable_bytes";
    useful.replace(useful.find(old_scope), old_scope.size(), "persistent_catalog_useful_field_bytes_v1");
    useful.pop_back();
    useful += ",\"key_payload_bytes\":" + std::to_string(key_bytes) + "}";
    allocated.pop_back();
    return allocated + ",\"useful\":" + useful + "}";
}
} // namespace ec::metadata_memory
