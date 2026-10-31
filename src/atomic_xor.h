#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace ec {
// Native parity bytes are updated with atomic read-modify-write instructions.
// A byte-sized operation has no alignment precondition, so sub-chunk ranges
// (including odd offsets and tails) use the same semantics as aligned ranges.
// The owner still pins the backing allocation and fences reads/transactions.
inline void atomic_xor(char* destination, std::string_view delta) noexcept {
    for (size_t i = 0; i < delta.size(); ++i)
        __atomic_fetch_xor(reinterpret_cast<unsigned char*>(destination + i),
                           static_cast<unsigned char>(delta[i]), __ATOMIC_RELAXED);
}
} // namespace ec
