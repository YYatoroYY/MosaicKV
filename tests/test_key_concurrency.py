"""Real TCP tests with a delayed read reply to hold an old fragment-map pin."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import queue
import os
import signal
import sys
import threading
import time
import unittest

import zmq
import test_mosaic_protocol as protocol
sys.path.insert(0, str(protocol.ROOT / 'benchmarks'))
from space_overhead import snapshot as space_snapshot


class ReadRelay:
    def __init__(self, target):
        self.target = target
        self.arm = threading.Event()
        self.captured = threading.Event()
        self.release = threading.Event()
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.replies = queue.Queue()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
        assert self.ready.wait(5)

    def run(self):
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.linger = 0
        self.port = frontend.bind_to_random_port('tcp://127.0.0.1')
        self.ready.set()
        def forward(route, request, hold):
            with context.socket(zmq.REQ) as sock:
                sock.linger = 0; sock.rcvtimeo = sock.sndtimeo = 10000
                sock.connect(self.target); sock.send_multipart(request)
                reply = sock.recv_multipart()
            if hold:
                self.captured.set()
                self.release.wait(10)
            self.replies.put(route + reply)
        with ThreadPoolExecutor(max_workers=16) as pool:
            while not self.stop.is_set():
                if frontend.poll(5):
                    message = frontend.recv_multipart()
                    route, request = message[:2], message[2:]
                    hold = (self.arm.is_set() and len(request) == 5 and request[2] == b'READ_RANGES')
                    if hold:
                        self.arm.clear()
                    pool.submit(forward, route, request, hold)
                while not self.replies.empty():
                    frontend.send_multipart(self.replies.get_nowait())
        frontend.close(); context.term()

    def hold_next(self):
        self.captured.clear(); self.release.clear(); self.arm.set()

    def close(self):
        self.release.set(); self.stop.set(); self.thread.join(15)


class KeyConcurrency(unittest.TestCase):
    PARITY = 2
    setUp = protocol.NativeCluster.setUp
    stop = protocol.NativeCluster.stop
    request = protocol.NativeCluster.request

    def configure_proxy(self, env, config, ports):
        self.relay = ReadRelay(self.endpoints[0])
        self.addCleanup(self.relay.close)
        routed = Path(config).with_name('proxy-nodes.conf')
        routed.write_text(''.join(f'{i} 127.0.0.1 {self.relay.port if i == 0 else port}\n'
                                  for i, port in enumerate(ports[:self.proxy])))
        env['NODES_FILE'] = str(routed)
        env['NODE_TIMEOUT_MS'] = '4000'
        env['MOSAIC_APPEND'] = '1'
        if self._testMethodName in ('test_degraded_read_waits_for_other_keys_parity', 'test_shared_stripe_concurrent_updates_and_aborts'):
            env['NODE_TIMEOUT_MS'] = '100'

    def stats(self):
        return json.loads(self.request(self.proxy, 'STATS'))

    def test_malformed_client_does_not_stop_dispatch(self):
        for target, frames in ((self.proxy, ['GET', 'key']), (1, ['READ', '1'])):
            result = self.request(target, *frames, *(['extra']*12), ok=False)
            self.assertEqual(result[0], b'INVALID')
            self.request(target, 'PING', *([str(target)] if target < self.proxy else []))

    def test_old_reader_survives_relocation_delete_and_reinsert(self):
        self.request(self.proxy, 'PUT', 'key', b'a'*64)
        with ThreadPoolExecutor(max_workers=3) as pool:
            for replacement in (b'b'*80, None):
                old = self.request(self.proxy, 'GET', 'key')
                self.relay.hold_next()
                reader = pool.submit(self.request, self.proxy, 'GET', 'key')
                self.assertTrue(self.relay.captured.wait(3))
                if replacement is None:
                    writer = pool.submit(self.request, self.proxy, 'DELETE', 'key')
                else:
                    writer = pool.submit(self.request, self.proxy, 'UPDATE', 'key', replacement)
                writer.result(timeout=3)  # Must finish while the old reader is pinned.
                self.assertFalse(reader.done())
                state = self.stats()
                self.assertEqual(state['reader_retained_data_bytes'], len(old))
                self.assertEqual(state['retired_fragment_maps'], 1)
                self.assertEqual(space_snapshot(state, 'mosaic')['reader_retained_data_bytes'], len(old))
                self.assertEqual(state['logical_live_bytes'] + state['free_data_bytes'] + len(old), state['reserved_data_bytes'])
                if replacement is None:
                    self.assertEqual(self.request(self.proxy, 'GET', 'key', ok=False)[0], b'NOT_FOUND')
                    self.request(self.proxy, 'PUT', 'key', b'new'*30)
                self.relay.release.set()
                self.assertEqual(reader.result(timeout=3), old)
                self.assertEqual(self.stats()['reader_retained_data_bytes'], 0)
        self.assertEqual(self.request(self.proxy, 'GET', 'key'), b'new'*30)

    def test_inplace_waits_for_its_reader_but_other_keys_progress(self):
        self.request(self.proxy, 'PUT', 'hot', b'a'*64)
        self.request(self.proxy, 'PUT', 'other', b'x'*70)
        with ThreadPoolExecutor(max_workers=3) as pool:
            self.relay.hold_next()
            reader = pool.submit(self.request, self.proxy, 'GET', 'hot')
            self.assertTrue(self.relay.captured.wait(3))
            writer = pool.submit(self.request, self.proxy, 'UPDATE', 'hot', b'b'*64)
            other = pool.submit(self.request, self.proxy, 'UPDATE', 'other', b'y'*90)
            other.result(timeout=3)
            self.assertFalse(writer.done())
            self.relay.release.set()
            self.assertEqual(reader.result(timeout=3), b'a'*64)
            writer.result(timeout=3)
        self.assertEqual(self.request(self.proxy, 'GET', 'hot'), b'b'*64)

    def test_retained_append_keeps_earlier_reader_epoch_alive(self):
        self.request(self.proxy, 'PUT', 'key', b'a'*64)
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.relay.hold_next()
            reader = pool.submit(self.request, self.proxy, 'GET', 'key')
            self.assertTrue(self.relay.captured.wait(3))
            self.request(self.proxy, 'APPEND', 'key', b'b'*30)
            self.request(self.proxy, 'UPDATE', 'key', b'c'*180)
            self.assertEqual(self.stats()['reader_retained_data_bytes'], 94)
            self.assertFalse(reader.done())
            self.relay.release.set()
            self.assertEqual(reader.result(timeout=3), b'a'*64)
        self.assertEqual(self.stats()['reader_retained_data_bytes'], 0)
        self.assertEqual(self.request(self.proxy, 'GET', 'key'), b'c'*180)

    def test_parallel_keys_and_hot_key_linearizable_values(self):
        for i in range(8):
            self.request(self.proxy, 'PUT', f'k{i}', bytes([i])*64)
        start = threading.Barrier(8)
        def work(i):
            start.wait()
            for step in range(16):
                value = bytes([i+step])* (64 if step % 2 == 0 else 190)
                self.request(self.proxy, 'UPDATE', f'k{i}', value)
                self.assertEqual(self.request(self.proxy, 'GET', f'k{i}'), value)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(work, range(8)))
        self.assertGreater(self.stats()['max_concurrent_updates'], 1)
        self.request(self.proxy, 'PUT', 'hot', b'a'*900)
        start = threading.Barrier(6)
        def contend(i):
            start.wait()
            for step in range(12):
                if i < 3:
                    self.request(self.proxy, 'UPDATE', 'hot', bytes([97+i])*(900 if step % 2 else 600+i*100))
                else:
                    value = self.request(self.proxy, 'GET', 'hot')
                    self.assertIn(value, (b'a'*900, b'b'*900, b'c'*900, b'a'*600, b'b'*700, b'c'*800))
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(contend, range(6)))
        state = self.stats()
        self.assertFalse(state['pending_update'])
        self.assertEqual(state['reader_retained_data_bytes'], 0)
        self.assertEqual(state['logical_live_bytes'] + state['free_data_bytes'], state['reserved_data_bytes'])

    def test_degraded_read_waits_for_other_keys_parity(self):
        for i in range(6):
            self.request(self.proxy, 'PUT', f'full{i}', bytes([65+i])*256)
        a = json.loads(self.request(self.proxy, 'META', 'full0'))
        b = json.loads(self.request(self.proxy, 'META', 'full5'))
        self.assertEqual(a['extents'][0]['stripe'], b['extents'][0]['stripe'])
        self.assertEqual(b['node'], 1)
        os.kill(self.processes[3].pid, signal.SIGSTOP)
        try:
            self.assertNotEqual(self.request(self.proxy, 'UPDATE', 'full0', b'z'*256, ok=False)[0], b'OK')
            os.kill(self.processes[1].pid, signal.SIGSTOP)
            try:
                # The same stripe has an unresolved update on a different key.
                # Decoding must fail closed, not combine mismatched generations.
                self.assertNotEqual(self.request(self.proxy, 'GET', 'full5', ok=False)[0], b'OK')
            finally:
                os.kill(self.processes[1].pid, signal.SIGCONT)
        finally:
            os.kill(self.processes[3].pid, signal.SIGCONT)
        self.assertEqual(self.request(self.proxy, 'GET', 'full0'), b'A'*256)
        self.assertEqual(self.request(self.proxy, 'GET', 'full5'), b'F'*256)

    def test_shared_stripe_concurrent_updates_and_aborts(self):
        for i in range(6):
            self.request(self.proxy, 'PUT', f'full{i}', bytes([65+i])*256)
        a = json.loads(self.request(self.proxy, 'META', 'full0'))
        b = json.loads(self.request(self.proxy, 'META', 'full5'))
        self.assertEqual(a['extents'][0]['stripe'], b['extents'][0]['stripe'])
        start = threading.Barrier(2)
        def update(key, byte):
            for _ in range(6):
                start.wait()
                self.request(self.proxy, 'UPDATE', key, bytes([byte])*256)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(update, 'full0', 88), pool.submit(update, 'full5', 89)]
            for job in jobs:
                job.result(timeout=10)
            os.kill(self.processes[3].pid, signal.SIGSTOP)
            try:
                jobs = [pool.submit(self.request, self.proxy, 'UPDATE', key, b'z'*256, ok=False)
                        for key in ('full0', 'full5')]
                for job in jobs:
                    self.assertNotEqual(job.result(timeout=5)[0], b'OK')
            finally:
                os.kill(self.processes[3].pid, signal.SIGCONT)
            jobs = [pool.submit(self.request, self.proxy, 'GET', key) for key in ('full0', 'full5')]
            self.assertEqual([j.result(timeout=5) for j in jobs], [b'X'*256, b'Y'*256])
        # Both data positions contributed overlapping parity deltas. Decode
        # each in turn to verify both commits and both inverse aborts composed.
        for node, key, value in ((0, 'full0', b'X'*256), (1, 'full5', b'Y'*256)):
            os.kill(self.processes[node].pid, signal.SIGSTOP)
            try:
                self.assertEqual(self.request(self.proxy, 'GET', key), value)
            finally:
                os.kill(self.processes[node].pid, signal.SIGCONT)


if __name__ == '__main__':
    unittest.main()
