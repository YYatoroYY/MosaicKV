#pragma once
#include "common.h"
#include <isa-l.h>
#include <array>
#include <string_view>

namespace ec {
class Codec {
    int k_, m_, bytes_;
    std::vector<unsigned char> matrix_, tables_;
    std::vector<std::vector<unsigned char>> delta_tables_;
public:
    Codec(int k, int m, int bytes) : k_(k), m_(m), bytes_(bytes),
        matrix_((k + m) * k), tables_(32 * k * m) {
        require(k > 0 && m > 0 && k + m <= 32 && bytes >= 64, "invalid EC configuration");
        // ISA-L expects the entire (k+m) x k generator matrix, including identity rows.
        gf_gen_cauchy1_matrix(matrix_.data(), k + m, k);
        ec_init_tables(k, m, matrix_.data() + k * k, tables_.data());
        for (int i = 0; i < k; ++i) {
            std::vector<unsigned char> coefficients(m);
            for (int j = 0; j < m; ++j) coefficients[j] = matrix_[(k + j) * k + i];
            delta_tables_.emplace_back(32 * m);
            ec_init_tables(1, m, coefficients.data(), delta_tables_.back().data());
        }
    }
    int width() const { return k_ + m_; }
    // Outputs are caller-owned scratch and must not alias delta. ISA-L reads
    // its source despite accepting a non-const pointer; only short inputs need
    // a padded copy. Keep this storage out of the per-(K, B) codec cache.
    void parity_delta_into(int index, std::string_view delta, Frames& outputs) {
        require(index >= 0 && index < k_ && !delta.empty() && delta.size() <= static_cast<size_t>(bytes_), "invalid delta");
        std::array<unsigned char, 64> padded{};
        auto* input = reinterpret_cast<unsigned char*>(const_cast<char*>(delta.data()));
        if (delta.size() < padded.size()) {
            std::memcpy(padded.data(), delta.data(), delta.size());
            input = padded.data();
        }
        auto length = std::max<size_t>(64, delta.size());
        outputs.resize(m_);
        std::array<unsigned char*, 32> pointers{};
        for (int j = 0; j < m_; ++j) {
            outputs[j].resize(length);
            pointers[j] = reinterpret_cast<unsigned char*>(outputs[j].data());
        }
        ec_encode_data(length, 1, m_, delta_tables_[index].data(), &input, pointers.data());
        for (auto& output : outputs) output.resize(delta.size());
    }
    std::vector<std::string> parity_delta(int index, const std::string& delta) {
        Frames outputs;
        parity_delta_into(index, delta, outputs);
        return outputs;
    }
    std::vector<std::string> encode(const std::string& data) {
        require(data.size() <= static_cast<size_t>(k_) * bytes_, "stripe too large");
        std::vector<std::string> blocks(width(), std::string(bytes_, '\0'));
        for (size_t offset = 0; offset < data.size(); offset += bytes_)
            blocks[offset / bytes_].replace(0, std::min<size_t>(bytes_, data.size() - offset), data, offset,
                                           std::min<size_t>(bytes_, data.size() - offset));
        std::vector<unsigned char*> pointers;
        for (auto& block : blocks) pointers.push_back(reinterpret_cast<unsigned char*>(block.data()));
        ec_encode_data(bytes_, k_, m_, tables_.data(), pointers.data(), pointers.data() + k_);
        return blocks;
    }
    // Decode only one shard's requested bytes. Concatenated fragment ranges are
    // valid inputs because RS operates independently at each byte offset.
    std::string recover_target(const std::vector<std::string>& blocks, int target, size_t length) {
        require(static_cast<int>(blocks.size()) == width() && target >= 0 && target < width()
                && length > 0 && length <= MAX_VALUE, "invalid range recovery");
        std::vector<unsigned char> selected(k_ * k_), inverse(k_ * k_), row(k_), tables(32 * k_);
        std::vector<unsigned char*> sources;
        std::vector<std::string> padded;
        padded.reserve(k_);
        const auto count = std::max<size_t>(64, length);
        for (int i = 0; i < width() && static_cast<int>(sources.size()) < k_; ++i) {
            if (blocks[i].size() != length) continue;
            std::copy_n(matrix_.data() + i * k_, k_, selected.data() + sources.size() * k_);
            if (length < count) {
                padded.push_back(blocks[i]); padded.back().resize(count, '\0');
                sources.push_back(reinterpret_cast<unsigned char*>(padded.back().data()));
            } else sources.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(blocks[i].data())));
        }
        if (static_cast<int>(sources.size()) < k_) throw Error("UNAVAILABLE", "fewer than K valid fragment sources");
        if (gf_invert_matrix(selected.data(), inverse.data(), k_) < 0)
            throw Error("UNAVAILABLE", "singular fragment decoding matrix");
        for (int col = 0; col < k_; ++col)
            for (int j = 0; j < k_; ++j)
                row[col] ^= gf_mul(matrix_[target * k_ + j], inverse[j * k_ + col]);
        ec_init_tables(k_, 1, row.data(), tables.data());
        std::string result(count, '\0');
        auto* destination = reinterpret_cast<unsigned char*>(result.data());
        ec_encode_data(count, k_, 1, tables.data(), sources.data(), &destination);
        result.resize(length);
        return result;
    }
    std::vector<std::string> recover(const std::vector<std::string>& blocks) {
        require(static_cast<int>(blocks.size()) == width(), "wrong stripe width");
        std::vector<unsigned char> selected(k_ * k_), inverse(k_ * k_), tables(32 * k_ * k_);
        std::vector<unsigned char*> sources;
        for (int i = 0; i < width() && static_cast<int>(sources.size()) < k_; ++i) {
            if (static_cast<int>(blocks[i].size()) != bytes_) continue;
            std::copy_n(matrix_.data() + i * k_, k_, selected.data() + sources.size() * k_);
            sources.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(blocks[i].data())));
        }
        if (static_cast<int>(sources.size()) < k_) throw Error("UNAVAILABLE", "fewer than K valid shards");
        if (gf_invert_matrix(selected.data(), inverse.data(), k_) < 0)
            throw std::runtime_error("singular decoding matrix");
        ec_init_tables(k_, k_, inverse.data(), tables.data());
        std::vector<std::string> data(k_, std::string(bytes_, '\0'));
        std::vector<unsigned char*> destinations;
        for (auto& block : data) destinations.push_back(reinterpret_cast<unsigned char*>(block.data()));
        ec_encode_data(bytes_, k_, k_, tables.data(), sources.data(), destinations.data());
        std::string joined;
        for (auto& block : data) joined += block;
        return encode(joined);
    }
};
} // namespace ec
