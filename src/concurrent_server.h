#pragma once
#include "common.h"
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

namespace ec {
// The ROUTER and completion PULL belong to the dispatch thread. Each worker
// owns its PUSH socket. A separate pool for node coordinators guarantees that
// parity/control requests can run while coordinators wait for remote nodes.
template<class Handler, class Slow>
void serve_concurrent(Context& context, const std::string& endpoint, size_t count, Handler handler, Slow slow) {
    struct Work { Frames route, request; };
    struct Queue { std::mutex mutex; std::condition_variable ready; std::deque<Work> jobs; bool stop = false; };
    Queue queues[2];
    const auto channel = "inproc://replies-" + uuid();
    Socket server(context, ZMQ_ROUTER, -1), completed(context, ZMQ_PULL, -1);
    server.bind(endpoint); completed.bind(channel);
    std::vector<std::thread> workers;
    auto shutdown = [&] {
        for (auto& q : queues) { { std::lock_guard<std::mutex> l(q.mutex); q.stop = true; } q.ready.notify_all(); }
        for (auto& w : workers) if (w.joinable()) w.join();
    };
    try {
        for (size_t pool = 0; pool < 2; ++pool) for (size_t i = 0; i < count; ++i)
            workers.emplace_back([&, pool] {
                Socket output(context, ZMQ_PUSH, -1); output.connect(channel);
                auto& q = queues[pool];
                for (;;) {
                    Work work;
                    { std::unique_lock<std::mutex> l(q.mutex);
                      q.ready.wait(l, [&] { return q.stop || !q.jobs.empty(); });
                      if (q.jobs.empty()) return;
                      work = std::move(q.jobs.front()); q.jobs.pop_front(); }
                    Frames reply;
                    try { reply = handler(work.request); }
                    catch (const Error& e) { reply = {e.status, e.what()}; }
                    catch (const std::exception& e) { reply = {"INTERNAL", e.what()}; }
                    work.route.insert(work.route.end(), std::make_move_iterator(reply.begin()), std::make_move_iterator(reply.end()));
                    send(output, work.route);
                }
            });
        for (;;) {
            zmq_pollitem_t items[] = {{server.ptr, 0, ZMQ_POLLIN, 0}, {completed.ptr, 0, ZMQ_POLLIN, 0}};
            if (zmq_poll(items, 2, -1) < 0) { if (errno == EINTR) continue; throw std::runtime_error("dispatcher poll failed"); }
            if (items[1].revents & ZMQ_POLLIN) send(server, receive(completed, 10));
            if (items[0].revents & ZMQ_POLLIN) {
                Frames message, rejected_route;
                try { message = receive(server, 10, &rejected_route); }
                catch (const Error& e) {
                    if (rejected_route.size() == 2 && rejected_route[1].empty())
                        send(server, {rejected_route[0], "", e.status, e.what()});
                    continue; // A malformed client must not stop the dispatcher.
                }
                if (message.size() < 3 || !message[1].empty()) continue;
                Work work{{std::move(message[0]), ""}, {}};
                work.request.assign(std::make_move_iterator(message.begin()+2), std::make_move_iterator(message.end()));
                auto& q = queues[slow(work.request) ? 1 : 0];
                { std::lock_guard<std::mutex> l(q.mutex); q.jobs.push_back(std::move(work)); }
                q.ready.notify_one();
            }
        }
    } catch (...) { shutdown(); throw; }
}
} // namespace ec
