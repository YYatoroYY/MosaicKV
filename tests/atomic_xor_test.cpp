#include "atomic_xor.h"
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

int main() {
    try {
        std::string target(133, '\x5a'), expected = target;
        std::vector<std::string> deltas;
        for (unsigned i = 1; i <= 7; ++i) {
            deltas.emplace_back(129, static_cast<char>(i * 17));
            for (size_t j = 0; j < 129; ++j) expected[1 + j] ^= deltas.back()[j];
        }
        std::vector<std::thread> workers;
        for (size_t i = 0; i < deltas.size(); ++i)
            workers.emplace_back([&, i] {
                for (int n = 0; n < 1001; ++n) ec::atomic_xor(target.data() + 1, deltas[i]);
            });
        for (auto& worker : workers) worker.join();
        if (target != expected) throw std::runtime_error("concurrent atomic XOR lost an update");
        for (const auto& delta : deltas) ec::atomic_xor(target.data() + 1, delta);
        if (target != std::string(133, '\x5a')) throw std::runtime_error("inverse XOR failed");
        std::cout << "PASS: overlapping atomic XOR, odd offsets and rollback inverse\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
