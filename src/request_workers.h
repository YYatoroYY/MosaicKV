#pragma once
#include "common.h"
#include "traffic.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

namespace ec {
// One sender/REQ socket per node. Callers may be transient async tasks; socket
// creation, use and destruction all happen in the same persistent worker thread.
class RequestWorkers {
    struct Job { Frames request; int timeout; std::promise<Frames> result; RpcTraffic* traffic = nullptr; RpcTraffic* link = nullptr; };
    struct Worker {
        std::string endpoint;
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<Job> queue;
        bool stop = false;
        std::thread thread;
        std::atomic<uint64_t> tasks{0}, connections{0}, resets{0};
        std::atomic<uint64_t> outstanding{0};
        explicit Worker(std::string address) : endpoint(std::move(address)) {}
    };
    Context& context_;
    RpcTrafficGroup links_; // Request initiator records each RPC once, by destination.
    std::vector<std::unique_ptr<Worker>> workers_;
    size_t lanes_;
    std::array<std::atomic<uint64_t>, 256> cursors_{};
    std::mutex selection_mutex_;
    // Same lock order as ParityWorkers: stop_mutex_ -> queue mutex. Network I/O
    // and promise completion hold neither; workers never acquire a Store lock.
    std::mutex stop_mutex_;
    void run(Worker& worker) {
        std::unique_ptr<Socket> socket;
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
            try {
                if (!socket) {
                    socket = std::make_unique<Socket>(context_, ZMQ_REQ, job.timeout);
                    socket->connect(worker.endpoint);
                    ++worker.connections;
                }
                // RMW has a longer reply deadline than ordinary node requests.
                if (zmq_setsockopt(socket->ptr, ZMQ_SNDTIMEO, &job.timeout, sizeof(job.timeout)) ||
                    zmq_setsockopt(socket->ptr, ZMQ_RCVTIMEO, &job.timeout, sizeof(job.timeout)))
                    throw Error("UNAVAILABLE", "cannot set request timeout");
                send(*socket, job.request);
                if (job.link) {
                    auto bytes = frame_bytes(job.request);
                    job.link->sent(bytes);
                    if (job.traffic) job.traffic->sent(bytes);
                }
                auto reply = receive(*socket);
                if (job.link) {
                    auto bytes = frame_bytes(reply);
                    job.link->received(bytes);
                    if (job.traffic) job.traffic->received(bytes);
                }
                if (reply.size() != 2) throw Error("UNAVAILABLE", "invalid node response");
                // Docker may assign the old IP to a different node on restart.
                // A complete identity rejection must also discard cached DNS/IP.
                // Do not replay this job; the next request opens a fresh socket.
                if (reply[0] == "INVALID" && reply[1] == "wrong node identity or missing frames")
                    throw Error("UNAVAILABLE", "node identity changed; reconnect required");
                // Application errors are complete replies and leave REQ usable.
                job.result.set_value(std::move(reply));
            } catch (...) {
                socket.reset(); // Discard partial/late replies and re-resolve DNS next time.
                ++worker.resets;
                // No automatic resend: a lost reply does not imply an unapplied write.
                job.result.set_exception(std::current_exception());
            }
            --worker.outstanding;
        }
    }
public:
    RequestWorkers(Context& context, const std::vector<std::string>& endpoints, size_t lanes = 1) : context_(context), lanes_(lanes) {
        require(!endpoints.empty() && endpoints.size() <= 256, "invalid request worker count");
        require(lanes > 0 && lanes <= 32, "invalid request worker lane count");
        try {
            for (const auto& endpoint : endpoints) for (size_t lane = 0; lane < lanes_; ++lane) {
                workers_.push_back(std::make_unique<Worker>(endpoint));
                auto* worker = workers_.back().get();
                worker->thread = std::thread([this, worker] { run(*worker); });
            }
        } catch (...) { stop(); throw; }
    }
    RequestWorkers(const RequestWorkers&) = delete;
    RequestWorkers& operator=(const RequestWorkers&) = delete;
    std::future<Frames> submit(size_t node, Frames request, int timeout, RpcTraffic* traffic = nullptr) {
        require(timeout > 0 && !request.empty(), "invalid node request");
        const auto& op = request[0] == "MOSAIC" && request.size() == 5 ? request[2] : request[0];
        const bool read = op == "READ" || op == "READ_RANGES" || op == "READ_BATCH";
        auto* link = traffic_enabled() && traffic_operation(request) ? links_.node(node) : nullptr;
        Job job{std::move(request), timeout, {}, traffic, link};
        auto result = job.result.get_future();
        // Reserve read lanes: a delayed reader must not hold up allocation,
        // commit or release for a writer publishing a different version.
        const size_t begin = lanes_ >= 4 && read ? lanes_/2 : 0;
        const size_t end = lanes_ >= 4 && !read ? lanes_/2 : lanes_;
        size_t selected;
        { std::lock_guard<std::mutex> selecting(selection_mutex_);
          selected = begin + cursors_.at(node).fetch_add(1) % (end-begin);
          for (size_t lane = begin; lane < end; ++lane)
              if (workers_.at(node*lanes_+lane)->outstanding < workers_.at(node*lanes_+selected)->outstanding) selected = lane;
          ++workers_.at(node*lanes_+selected)->outstanding; }
        auto& worker = *workers_.at(node * lanes_ + selected);
        try {
            std::lock_guard<std::mutex> guard(worker.mutex);
            if (worker.stop) throw std::runtime_error("request workers stopped");
            worker.queue.push_back(std::move(job));
        } catch (...) { --worker.outstanding; throw; }
        worker.ready.notify_one();
        return result;
    }
    void stop() {
        std::lock_guard<std::mutex> stopping(stop_mutex_);
        for (auto& worker : workers_) {
            { std::lock_guard<std::mutex> guard(worker->mutex); worker->stop = true; }
            worker->ready.notify_all();
        }
        for (auto& worker : workers_) if (worker->thread.joinable()) worker->thread.join();
    }
    ~RequestWorkers() { stop(); }
    std::string stats() const {
        std::ostringstream out;
        out << "{\"traffic_enabled\":" << (traffic_enabled() ? "true" : "false")
            << ",\"request_workers\":" << workers_.size() << ",\"workers\":[";
        for (size_t i = 0; i < workers_.size(); ++i)
            out << (i ? "," : "") << "{\"node\":" << i / lanes_ << ",\"tasks\":" << workers_[i]->tasks
                << ",\"connections\":" << workers_[i]->connections << ",\"resets\":" << workers_[i]->resets << '}';
        out << "],\"rpc_links\":" << links_.stats(workers_.size() / lanes_) << '}';
        return out.str();
    }
};
} // namespace ec
