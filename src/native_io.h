#pragma once
#include "common.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <sys/syscall.h>

namespace ec {
// A synchronous batch facade over persistent workers. The calling REP thread
// must not mutate the storage directory until run() returns (also on failure).
class NativeIo {
public:
    enum class Op { Read, Delta, Write, Xor };
private:
    struct Job { Op op; std::packaged_task<void()> task; };
    struct Worker {
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<Job> jobs;
        bool stop = false;
        std::thread thread;
        std::promise<void> started;
        std::atomic<long> tid{0};
        std::atomic<uint64_t> tasks{0}, reads{0}, writes{0};
    };
    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<int> active_{0}, max_active_{0};
    uint64_t batches_ = 0;
    void work(Worker& worker) {
        worker.tid = syscall(SYS_gettid);
        worker.started.set_value();
        for (;;) {
            Job job;
            {
                std::unique_lock<std::mutex> lock(worker.mutex);
                worker.ready.wait(lock, [&] { return worker.stop || !worker.jobs.empty(); });
                if (worker.jobs.empty()) return;
                job = std::move(worker.jobs.front()); worker.jobs.pop_front();
            }
            ++worker.tasks;
            if (job.op != Op::Write) ++worker.reads;
            if (job.op == Op::Write || job.op == Op::Xor) ++worker.writes;
            auto active = ++active_, previous = max_active_.load();
            while (active > previous && !max_active_.compare_exchange_weak(previous, active)) {}
            job.task(); // packaged_task delivers callback exceptions to the caller.
            --active_;
        }
    }
    void stop() noexcept {
        for (auto& worker : workers_) {
            { std::lock_guard<std::mutex> lock(worker->mutex); worker->stop = true; }
            worker->ready.notify_all();
        }
        for (auto& worker : workers_) if (worker->thread.joinable()) worker->thread.join();
    }
public:
    explicit NativeIo(size_t count) {
        require(count > 0 && count <= 32, "invalid native I/O thread count");
        try {
            workers_.reserve(count);
            std::vector<std::future<void>> starts;
            starts.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                workers_.push_back(std::make_unique<Worker>());
                auto* worker = workers_.back().get();
                starts.push_back(worker->started.get_future());
                worker->thread = std::thread([this, worker] { work(*worker); });
            }
            for (auto& start : starts) start.get();
        } catch (...) { stop(); throw; }
    }
    NativeIo(const NativeIo&) = delete;
    NativeIo& operator=(const NativeIo&) = delete;
    ~NativeIo() { stop(); }
    void run(size_t count, Op op, const std::function<void(size_t)>& action) {
        // Reserve before submitting. Every accepted task is joined on every
        // exit path, including an allocation failure while enqueueing a batch.
        std::vector<std::future<void>> replies;
        replies.reserve(count);
        std::mutex gate_mutex;
        std::condition_variable gate;
        bool ready = false, cancelled = false;
        auto open_gate = [&](bool cancel) {
            { std::lock_guard<std::mutex> lock(gate_mutex); cancelled = cancel; ready = true; }
            gate.notify_all();
        };
        try {
            for (size_t i = 0; i < count; ++i) {
                Job job{op, std::packaged_task<void()>([&, i] {
                    { std::unique_lock<std::mutex> lock(gate_mutex);
                      gate.wait(lock, [&] { return ready; });
                      if (cancelled) return; }
                    action(i);
                })};
                auto reply = job.task.get_future();
                auto& worker = *workers_[i % workers_.size()];
                {
                    std::lock_guard<std::mutex> lock(worker.mutex);
                    worker.jobs.push_back(std::move(job));
                }
                replies.push_back(std::move(reply));
                worker.ready.notify_one();
            }
        } catch (...) {
            open_gate(true); // No range is changed after a partial enqueue failure.
            for (auto& reply : replies) reply.wait();
            throw;
        }
        open_gate(false);
        ++batches_;
        std::exception_ptr failure;
        for (auto& reply : replies) {
            try { reply.get(); } catch (...) { if (!failure) failure = std::current_exception(); }
        }
        if (failure) std::rethrow_exception(failure);
    }
    std::string stats() const {
        std::ostringstream out;
        out << "{\"kind\":\"native_ranges\",\"threads\":" << workers_.size()
            << ",\"parallel_batches\":" << batches_ << ",\"max_active\":" << max_active_.load() << ",\"workers\":[";
        for (size_t i = 0; i < workers_.size(); ++i) {
            const auto& w = *workers_[i];
            out << (i ? "," : "") << "{\"tid\":" << w.tid.load() << ",\"tasks\":" << w.tasks.load()
                << ",\"reads\":" << w.reads.load() << ",\"writes\":" << w.writes.load() << '}';
        }
        return out.str() + "]}";
    }
};
} // namespace ec
