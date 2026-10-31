#pragma once
#include "common.h"
#include <string_view>
#include <initializer_list>

namespace ec {
// Views borrow the caller's immutable record. Never queue or retain them after
// that record's lifetime; worker requests must still own their serialized bytes.
using RecordViews = std::vector<std::string_view>;
inline RecordViews unpack_record_views(std::string_view raw) {
    RecordViews frames;
    for (size_t pos = 0; pos < raw.size();) {
        if (raw.size() - pos < 4) throw std::runtime_error("truncated record length");
        uint32_t size = 0;
        for (int i = 0; i < 4; ++i) size = (size << 8) | static_cast<unsigned char>(raw[pos++]);
        if (size > raw.size() - pos) throw std::runtime_error("truncated record field");
        frames.push_back(raw.substr(pos, size));
        pos += size;
    }
    return frames;
}
inline void append_record_length(std::string& output, size_t length) {
    require(length <= UINT32_MAX, "record field too large");
    auto n = static_cast<uint32_t>(length);
    for (int shift = 24; shift >= 0; shift -= 8) output += static_cast<char>(n >> shift);
}
inline void append_record_field(std::string& output, std::string_view field) {
    append_record_length(output, field.size());
    if (!field.empty()) output.append(field.data(), field.size());
}
template<class Range> inline std::string pack_record_views(const Range& frames) {
    size_t bytes = 0;
    for (const auto& frame : frames) {
        require(frame.size() <= UINT32_MAX && bytes <= SIZE_MAX - 4 - frame.size(), "record too large");
        bytes += 4 + frame.size();
    }
    std::string output;
    output.reserve(bytes);
    for (const auto& frame : frames) append_record_field(output, frame);
    return output;
}
inline std::string pack_record_views(std::initializer_list<std::string_view> frames) {
    return pack_record_views<std::initializer_list<std::string_view>>(frames);
}
} // namespace ec
