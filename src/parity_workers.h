#pragma once
#include "common.h"
#include "traffic.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <pthread.h>
#include <sys/syscall.h>

namespace ec {
// A promise-backed future does not join its producer on destruction. Keep accepted
// jobs alive and drain them on every exit, including an exception in a later submit.
class PendingReplies {
    std::vector<std::future<Frames>> replies_;
public:
    explicit PendingReplies(size_t count) { replies_.reserve(count); }
    PendingReplies(const PendingReplies&) = delete;
    PendingReplies& operator=(const PendingReplies&) = delete;
    ~PendingReplies() {
        for (auto& reply : replies_) if (reply.valid()) reply.wait();
    }
    void add(std::future<Frames> reply) {
        try { replies_.push_back(std::move(reply)); }
        catch (...) { if (reply.valid()) reply.wait(); throw; }
    }
    auto begin() { return replies_.begin(); }
    auto end() { return replies_.end(); }
};

// One long-lived sender per parity index. Sockets never leave their owning thread.
class ParityWorkers {
    struct Job { std::string endpoint; Frames request; std::promise<Frames> result; RpcTraffic* traffic = nullptr; RpcTraffic* link = nullptr; bool count_traffic = false; };
    struct Worker {
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<Job> queue;
        bool stop = false;
        std::thread thread;
        std::atomic<long> tid{0};
        std::atomic<uint64_t> tasks{0}, connections{0};
        std::atomic<uint64_t> sent_bytes{0}, received_bytes{0};
    };
    Context& context_;
    RpcTrafficGroup links_;
    std::atomic<size_t> destination_count_{0};
    int timeout_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<int> active_{0}, max_active_{0};
    // Lock order: stop_mutex_ -> Worker::mutex. Workers never acquire stop_mutex_,
    // workers release the queue mutex before network I/O/promise completion.
    // stop() releases every queue mutex before joining while holding stop_mutex_.
    std::mutex stop_mutex_;
    void run(Worker& worker, int index) {
        pthread_setname_np(pthread_self(), ("ec-parity-" + std::to_string(index)).c_str());
        {
            std::lock_guard<std::mutex> guard(worker.mutex);
            worker.tid = syscall(SYS_gettid);
        }
        worker.ready.notify_all();
        std::map<std::string, std::unique_ptr<Socket>> sockets;
        while (true) {
            Job job;
            {
                std::unique_lock<std::mutex> guard(worker.mutex);
                worker.ready.wait(guard, [&] { return worker.stop || !worker.queue.empty(); });
                if (worker.queue.empty()) return;
                job = std::move(worker.queue.front());
                worker.queue.pop_front();
            }
            ++worker.tasks;
            int active = ++active_, previous = max_active_;
            while (active > previous && !max_active_.compare_exchange_weak(previous, active)) {}
            try {
                Frames reply;
                // PATCH/PATCH_BATCH/PATCH_RESIZE apply XOR without transaction logs. A lost
                // reply must propagate to the caller, never apply it twice.
                const int attempts = (job.request[0] == "PATCH" || job.request[0] == "PATCH_BATCH" || job.request[0] == "PATCH_RESIZE") ? 1 : 2;
                for (int attempt = 0; attempt < attempts; ++attempt) {
                    try {
                        auto& socket = sockets[job.endpoint];
                        if (!socket) {
                            socket = std::make_unique<Socket>(context_, ZMQ_REQ, timeout_);
                            socket->connect(job.endpoint);
                            ++worker.connections;
                        }
                        send(*socket, job.request);
                        if (job.count_traffic) {
                            auto bytes = frame_bytes(job.request);
                            if (job.link) job.link->sent(bytes);
                            if (job.traffic) job.traffic->sent(bytes);
                            worker.sent_bytes.fetch_add(bytes, std::memory_order_relaxed);
                        }
                        reply = receive(*socket);
                        if (job.count_traffic) {
                            auto bytes = frame_bytes(reply);
                            if (job.link) job.link->received(bytes);
                            if (job.traffic) job.traffic->received(bytes);
                            worker.received_bytes.fetch_add(bytes, std::memory_order_relaxed);
                        }
                        if (reply.size() != 2 || reply[0] != "OK")
                            throw Error("UNAVAILABLE", "parity write failed: " + (reply.size() == 2 ? reply[1] : "bad response"));
                        break;
                    } catch (const Error&) {
                        // Re-resolve DNS after a container restart; transaction IDs make retry safe.
                        sockets.erase(job.endpoint);
                        if (attempt + 1 == attempts) throw;
                    }
                }
                --active_;
                job.result.set_value(std::move(reply));
            } catch (...) {
                sockets.erase(job.endpoint); // A timed-out REQ must be recreated before retry.
                --active_;
                job.result.set_exception(std::current_exception());
            }
        }
    }
public:
    ParityWorkers(Context& context, int count, int timeout) : context_(context), timeout_(timeout) {
        require(count > 0 && count <= 31, "invalid parity worker count");
        try {
            for (int i = 0; i < count; ++i) {
                workers_.push_back(std::make_unique<Worker>());
                Worker& worker = *workers_.back();
                worker.thread = std::thread([this, target = &worker, i] { run(*target, i); });
                std::unique_lock<std::mutex> guard(worker.mutex);
                worker.ready.wait(guard, [&] { return worker.tid.load() != 0; });
            }
        } catch (...) { stop(); throw; }
    }
    void stop() {
        std::lock_guard<std::mutex> stopping(stop_mutex_); // Serialize concurrent joins.
        for (auto& worker : workers_) {
            { std::lock_guard<std::mutex> guard(worker->mutex); worker->stop = true; }
            worker->ready.notify_all();
        }
        for (auto& worker : workers_) if (worker->thread.joinable()) worker->thread.join();
    }
    ~ParityWorkers() { stop(); }
    std::future<Frames> submit(int index, std::string endpoint, Frames request, RpcTraffic* traffic = nullptr, int destination = -1) {
        auto& worker = *workers_.at(index);
        bool count_traffic = traffic_enabled() && traffic_operation(request);
        RpcTraffic* link = count_traffic && destination >= 0 ? links_.node(destination) : nullptr;
        if (link) {
            size_t count = static_cast<size_t>(destination) + 1, previous = destination_count_;
            while (count > previous && !destination_count_.compare_exchange_weak(previous, count)) {}
        }
        Job job{std::move(endpoint), std::move(request), {}, traffic, link, count_traffic};
        auto result = job.result.get_future();
        {
            std::lock_guard<std::mutex> guard(worker.mutex);
            if (worker.stop) throw std::runtime_error("parity workers stopped");
            worker.queue.push_back(std::move(job));
        }
        worker.ready.notify_one();
        return result;
    }
    std::string stats() const {
        std::ostringstream out;
        out << "{\"traffic_enabled\":" << (traffic_enabled() ? "true" : "false")
            << ",\"parity_workers\":" << workers_.size() << ",\"max_parallel_writes\":" << max_active_ << ",\"workers\":[";
        for (size_t i = 0; i < workers_.size(); ++i)
            out << (i ? "," : "") << "{\"index\":" << i << ",\"tid\":" << workers_[i]->tid
                << ",\"tasks\":" << workers_[i]->tasks << ",\"connections\":" << workers_[i]->connections
                << ",\"sent_bytes\":" << workers_[i]->sent_bytes << ",\"received_bytes\":" << workers_[i]->received_bytes << '}';
        out << "],\"rpc_links\":" << links_.stats(destination_count_.load()) << '}';
        return out.str();
    }
    uint64_t sent_bytes() const {
        uint64_t bytes = 0;
        for (const auto& worker : workers_) bytes += worker->sent_bytes.load(std::memory_order_relaxed);
        return bytes;
    }
    uint64_t received_bytes() const {
        uint64_t bytes = 0;
        for (const auto& worker : workers_) bytes += worker->received_bytes.load(std::memory_order_relaxed);
        return bytes;
    }
};
} // namespace ec
