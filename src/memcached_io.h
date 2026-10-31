#pragma once
#include "memcached_client.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <sys/syscall.h>

namespace ec {
// The caller must drain every accepted job before reclaiming a generation.
class CacheBatch {
    std::vector<std::future<std::string>> replies_;
    std::shared_ptr<std::atomic<bool>> cancelled_ = std::make_shared<std::atomic<bool>>(false);
public:
    explicit CacheBatch(size_t count) { replies_.reserve(count); }
    ~CacheBatch() { for (auto& f : replies_) if (f.valid()) f.wait(); }
    void add(std::future<std::string> f) {
        try { replies_.push_back(std::move(f)); }
        catch (...) { if (f.valid()) f.wait(); throw; }
    }
    auto begin() { return replies_.begin(); }
    auto end() { return replies_.end(); }
    auto token() const { return cancelled_; }
};

class MemcachedIo {
public:
    enum class Op { Get, Add, Put, Delete, Fence };
private:
    struct Job { Op op; std::string key, value; std::shared_ptr<std::atomic<bool>> cancelled; std::promise<std::string> result; };
    struct Worker {
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<Job> jobs;
        bool stop = false;
        std::thread thread;
        std::promise<void> started;
        std::atomic<uint64_t> tasks{0}, reads{0}, writes{0}, deletes{0};
        std::atomic<long> tid{0};
    };
    std::vector<std::unique_ptr<Worker>> workers_;
    std::mutex stop_mutex_;
    std::atomic<int> active_{0}, max_active_{0};
    void run(Worker& w) {
        std::unique_ptr<MemcachedClient> client;
        try {
            client = std::make_unique<MemcachedClient>();
            w.tid = syscall(SYS_gettid);
            w.started.set_value();
        } catch (...) { w.started.set_exception(std::current_exception()); return; }
        for (;;) {
            Job job;
            {
                std::unique_lock<std::mutex> lock(w.mutex);
                w.ready.wait(lock, [&] { return w.stop || !w.jobs.empty(); });
                if (w.jobs.empty()) return;
                job = std::move(w.jobs.front()); w.jobs.pop_front();
            }
            ++w.tasks;
            if (job.cancelled && job.cancelled->load()) {
                job.result.set_exception(std::make_exception_ptr(Error("UNAVAILABLE", "chunk batch cancelled after a failed operation")));
                continue;
            }
            auto active = ++active_, previous = max_active_.load();
            while (active > previous && !max_active_.compare_exchange_weak(previous, active)) {}
            std::string result;
            std::exception_ptr error;
            try {
                switch (job.op) {
                case Op::Get: ++w.reads; result = client->get(job.key); break;
                case Op::Add: ++w.writes; client->add(job.key, job.value); break;
                case Op::Put: ++w.writes; client->put(job.key, job.value); break;
                case Op::Delete: ++w.deletes; client->erase(job.key); break;
                case Op::Fence: ++w.writes; client->put(job.key, ""); break;
                }
            } catch (...) {
                error = std::current_exception();
                if (job.cancelled) job.cancelled->store(true);
            }
            --active_;
            if (error) job.result.set_exception(error);
            else job.result.set_value(std::move(result));
        }
    }
public:
    explicit MemcachedIo(size_t count) {
        require(count > 0 && count <= 32, "invalid Agent I/O thread count");
        try {
            std::vector<std::future<void>> starts;
            starts.reserve(count); workers_.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                workers_.push_back(std::make_unique<Worker>());
                auto* w = workers_.back().get(); starts.push_back(w->started.get_future());
                w->thread = std::thread([this, w] { run(*w); });
            }
            for (auto& f : starts) f.get();
        } catch (...) { stop(); throw; }
    }
    MemcachedIo(const MemcachedIo&) = delete;
    MemcachedIo& operator=(const MemcachedIo&) = delete;
    size_t size() const { return workers_.size(); }
    std::future<std::string> submit(size_t lane, Op op, std::string key, std::string value = {},
                                    std::shared_ptr<std::atomic<bool>> cancelled = {}) {
        auto& w = *workers_.at(lane % workers_.size());
        Job job{op, std::move(key), std::move(value), std::move(cancelled), {}};
        auto future = job.result.get_future();
        {
            std::lock_guard<std::mutex> lock(w.mutex);
            if (w.stop) throw std::runtime_error("memcached workers stopped");
            w.jobs.push_back(std::move(job));
        }
        w.ready.notify_one();
        return future;
    }
    void stop() {
        std::lock_guard<std::mutex> stopping(stop_mutex_);
        for (auto& w : workers_) {
            { std::lock_guard<std::mutex> lock(w->mutex); w->stop = true; }
            w->ready.notify_all();
        }
        for (auto& w : workers_) if (w->thread.joinable()) w->thread.join();
    }
    ~MemcachedIo() { stop(); }
    std::string stats() const {
        std::ostringstream out;
        out << "{\"threads\":" << size() << ",\"max_active\":" << max_active_ << ",\"workers\":[";
        for (size_t i = 0; i < size(); ++i) {
            auto& w = *workers_[i];
            out << (i ? "," : "") << "{\"tid\":" << w.tid << ",\"tasks\":" << w.tasks
                << ",\"reads\":" << w.reads << ",\"writes\":" << w.writes << ",\"deletes\":" << w.deletes << '}';
        }
        out << "]}"; return out.str();
    }
};
} // namespace ec
