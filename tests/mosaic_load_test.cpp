#include "mosaic_load.h"
#include <cassert>
#include <iostream>
using ec::mosaic::CumulativeBandwidth;
int main() {
    CumulativeBandwidth parity(5, 100), data(5, 100);
    parity.record(3, 4096);
    parity.record(4, 8192);
    parity.record(3, 4096);
    data.record(0, 4096);
    assert(parity.bytes(3) == 8192 && parity.bytes(4) == 8192);
    assert(parity.bytes_per_second(3, 1100) == 8192);
    assert(parity.bytes_per_second(3, 2100) == 4096);
    assert(parity.bytes(0) == 0 && data.bytes(3) == 0);
    assert(data.bytes_per_second(0, 1100) == 4096);
    // Idle time changes the average denominator, never discards payload bytes.
    assert(parity.bytes_per_second(3, 1000100) == 8.192);
    assert(parity.bytes(3) == 8192);
    parity.record(3, 128);
    assert(parity.bytes(3) == 8320 && parity.bytes(4) == 8192);
    assert(parity.bytes_per_second(3, 100) == 8320000);
    parity.reset(2000); data.reset(2000);
    for (size_t i = 0; i < 5; ++i) assert(parity.bytes(i) == 0 && data.bytes(i) == 0);
    parity.record(3, 128);
    assert(parity.bytes_per_second(3, 3000) == 128);
    std::cout << "PASS cumulative data/parity bandwidth: independent roles, common epoch, no expiry, cumulative average\n";
}
