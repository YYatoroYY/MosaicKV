#pragma once
#include <zmq.h>
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/random.h>
#include <unistd.h>

namespace ec {
namespace fs = std::filesystem;
using Frames = std::vector<std::string>;
constexpr size_t MAX_KEY_BYTES = 128;
constexpr size_t MAX_VALUE = 16 * 1024 * 1024;
// Addressed chunk capacity is separate from user/fragment payload bytes.
// Four 12 MiB chunks fit; native RMW still touches only fragment ranges.
constexpr size_t MAX_BATCH_CHUNK_BYTES = 48 * 1024 * 1024;
// Full READ_BATCH replies must still fit the unchanged RPC message bound.
constexpr size_t MAX_BATCH_READ_BYTES = 24 * 1024 * 1024;
constexpr size_t MAX_BLOCK = 32 * 1024 * 1024; // Resized blocks may hold several values.
struct Error : std::runtime_error {
    std::string status;
    Error(std::string s, const std::string& why) : std::runtime_error(why), status(std::move(s)) {}
};
inline void require(bool ok, const std::string& why) {
    if (!ok) throw Error("INVALID", why);
}
inline std::string env(const char* name, const std::string& fallback) {
    const char* v = std::getenv(name);
    return v ? v : fallback;
}
// One process-wide backend setting; configure.py applies it to every service.
inline const std::string& storage_backend() {
    static const std::string backend = [] {
        auto value = env("STORAGE_BACKEND", "native");
        require(value == "memcached" || value == "native", "invalid STORAGE_BACKEND");
        return value;
    }();
    return backend;
}
inline int integer(const std::string& s, int lo, int hi) {
    size_t end = 0;
    long n;
    try { n = std::stol(s, &end); } catch (...) { throw Error("INVALID", "invalid integer: " + s); }
    require(end == s.size() && n >= lo && n <= hi, "integer out of range: " + s);
    return static_cast<int>(n);
}
inline std::string hex(const std::string& data) {
    const char* digits = "0123456789abcdef";
    std::string out;
    for (unsigned char c : data) { out += digits[c >> 4]; out += digits[c & 15]; }
    return out;
}
inline std::string json_string(const std::string& value) {
    static const char* digits = "0123456789abcdef";
    std::string out = "\"";
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') { out += '\\'; out += c; }
        else if (c < 32) {
            out += "\\u00"; out += digits[c >> 4]; out += digits[c & 15];
        } else out += c;
    }
    return out + '"';
}
inline std::string uuid() {
    std::string bytes(16, '\0');
    size_t pos = 0;
    while (pos < bytes.size()) {
        auto n = getrandom(bytes.data() + pos, bytes.size() - pos, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error("getrandom failed");
        pos += n;
    }
    return hex(bytes);
}
// Length-delimited records preserve binary keys/values without content checksums.
inline std::string pack_record(const Frames& frames) {
    std::string payload;
    for (const auto& frame : frames) {
        require(frame.size() <= UINT32_MAX, "record field too large");
        auto n = static_cast<uint32_t>(frame.size());
        for (int shift = 24; shift >= 0; shift -= 8) payload += static_cast<char>(n >> shift);
        payload += frame;
    }
    return payload;
}
inline Frames unpack_record(const std::string& raw) {
    Frames frames;
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
inline std::vector<std::string> load_nodes(const std::string& path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("cannot open NODES_FILE");
    std::vector<std::string> nodes;
    std::set<std::string> unique;
    std::string line;
    while (std::getline(input, line)) {
        std::istringstream parts(line.substr(0, line.find('#')));
        std::string id, host, port, extra;
        if (!(parts >> id)) continue;
        require(static_cast<bool>(parts >> host >> port) && !(parts >> extra), "nodes.conf: expected ID HOST PORT");
        require(integer(id, 0, 255) == static_cast<int>(nodes.size()), "node IDs must be ordered from zero");
        require(host.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-") == std::string::npos,
                "invalid node hostname");
        auto endpoint = "tcp://" + host + ":" + std::to_string(integer(port, 1, 65535));
        require(unique.insert(endpoint).second, "duplicate node endpoint");
        nodes.push_back(endpoint);
    }
    return nodes;
}
inline std::string read_file(const fs::path& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("cannot read " + path.string());
    std::string data((std::istreambuf_iterator<char>(f)), {});
    if (f.bad()) throw std::runtime_error("read failed: " + path.string());
    return data;
}
// Read resident memory usage from the process's kernel statistics.
inline uint64_t process_rss_bytes(pid_t pid = 0) {
    std::ifstream input("/proc/" + (pid ? std::to_string(pid) : std::string("self")) + "/statm");
    uint64_t virtual_pages = 0, resident_pages = 0;
    if (!(input >> virtual_pages >> resident_pages)) return 0;
    return resident_pages * static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
}
class Context {
public:
    void* ptr = zmq_ctx_new();
    Context() { if (!ptr) throw std::runtime_error("zmq_ctx_new failed"); }
    ~Context() { zmq_ctx_term(ptr); }
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;
};
class Socket {
public:
    void* ptr;
    Socket(Context& c, int type, int timeout = 1000) : ptr(zmq_socket(c.ptr, type)) {
        if (!ptr) throw std::runtime_error("zmq_socket failed");
        int linger = 0;
        int64_t max_size = MAX_BLOCK + 4096;
        zmq_setsockopt(ptr, ZMQ_LINGER, &linger, sizeof(linger));
        zmq_setsockopt(ptr, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
        zmq_setsockopt(ptr, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
        zmq_setsockopt(ptr, ZMQ_MAXMSGSIZE, &max_size, sizeof(max_size));
    }
    ~Socket() { zmq_close(ptr); }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    void bind(const std::string& endpoint) {
        if (zmq_bind(ptr, endpoint.c_str())) throw std::runtime_error("bind " + endpoint + ": " + zmq_strerror(errno));
    }
    void connect(const std::string& endpoint) {
        if (zmq_connect(ptr, endpoint.c_str())) throw Error("UNAVAILABLE", "connect " + endpoint);
    }
};
inline void send(Socket& socket, const Frames& parts) {
    for (size_t i = 0; i < parts.size(); ++i)
        if (zmq_send(socket.ptr, parts[i].data(), parts[i].size(), i + 1 < parts.size() ? ZMQ_SNDMORE : 0) < 0)
            throw Error("UNAVAILABLE", "send timeout or transport failure");
}
inline Frames receive(Socket& socket, size_t max_frames = 8, Frames* rejected_route = nullptr) {
    Frames parts;
    size_t total = 0, count = 0;
    bool oversized = false;
    int more;
    do {
        zmq_msg_t msg;
        zmq_msg_init(&msg);
        int rc = zmq_msg_recv(&msg, socket.ptr, 0);
        if (rc < 0) { zmq_msg_close(&msg); throw Error("UNAVAILABLE", "receive timeout or transport failure"); }
        size_t length = zmq_msg_size(&msg);
        total += length;
        oversized |= ++count > max_frames || total > MAX_BLOCK + 8192;
        if (!oversized) parts.emplace_back(static_cast<char*>(zmq_msg_data(&msg)), length);
        more = zmq_msg_more(&msg);
        zmq_msg_close(&msg);
    } while (more);
    if (oversized && rejected_route && parts.size() >= 2) *rejected_route = {parts[0], parts[1]};
    require(!oversized, "message too large or too many frames");
    return parts;
}
inline Frames rpc(Context& context, const std::string& endpoint, const Frames& request, int timeout) {
    Socket socket(context, ZMQ_REQ, timeout); // One-shot CLI helper; no automatic retry.
    socket.connect(endpoint);
    send(socket, request);
    return receive(socket);
}
template<class Handler> void serve(Socket& socket, Handler handler) {
    while (true) {
        Frames request;
        Frames reply;
        try { request = receive(socket); }
        catch (const Error& e) {
            if (e.status == "UNAVAILABLE") continue; // Interrupted receive; REP has no request.
            reply = {e.status, e.what()};
        }
        if (reply.empty()) {
            try { reply = handler(request); }
            catch (const Error& e) { reply = {e.status, e.what()}; }
            catch (const std::exception& e) { reply = {"INTERNAL", e.what()}; }
        }
        try { send(socket, reply); }
        catch (const std::exception& e) { throw std::runtime_error(std::string("reply failed: ") + e.what()); }
    }
}
} // namespace ec
