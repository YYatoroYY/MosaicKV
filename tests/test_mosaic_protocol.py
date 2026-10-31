"""Linux native-process tests: real proxy, nodes, ISA-L, TCP and fault fences."""
import json
import os
import random
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest

import zmq

ROOT = Path(__file__).resolve().parents[1]


def pack(*fields):
    fields = [x if isinstance(x, bytes) else str(x).encode() for x in fields]
    return b''.join(struct.pack('!I', len(x)) + x for x in fields)


class NativeCluster(unittest.TestCase):
    PARITY = 1

    def setUp(self):
        self.proxy = 2 + self.PARITY
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.context = zmq.Context()
        self.addCleanup(self.context.term)
        self.processes = []
        self.logs = []
        self.addCleanup(self.stop)
        ports = []
        while len(ports) < self.proxy + 1:
            with socket.socket() as s:
                s.bind(('127.0.0.1', 0))
                port = s.getsockname()[1]
                if port not in ports:
                    ports.append(port)
        self.endpoints = [f'tcp://127.0.0.1:{p}' for p in ports]
        config = Path(self.temp.name) / 'nodes.conf'
        config.write_text(''.join(f'{i} 127.0.0.1 {p}\n' for i, p in enumerate(ports[:self.proxy])))
        self.environment = dict(os.environ, EC_K='2', EC_M=str(self.PARITY), BLOCK_SIZE='256',
                                ALIGNMENT_CHUNK='256', MOSAIC_FMAX='4', MOSAIC_TAU='8',
                                MOSAIC_PLACEMENT='fixed', MOSAIC_SPACE_FALLBACK='none',
                                STORAGE_BACKEND='native', NODE_TIMEOUT_MS='100', STATS_TIMEOUT_MS='100',
                                NODES_FILE=str(config), EC_WORKLOAD_PHASE='resize',
                                MOSAIC_BATCH_IO='1', MOSAIC_PARALLEL_IO='1', UPDATE_STRATEGY='mosaic')
        if self._testMethodName == 'test_unified_placement_and_equal_size_map':
            self.environment['MOSAIC_PLACEMENT'] = 'parity_bandwidth'
            self.environment['MOSAIC_REBALANCE_EVERY'] = '1'
        for i in range(self.proxy + 1):
            log = open(Path(self.temp.name) / f'{i}.log', 'w+')
            self.logs.append(log)
            env = dict(self.environment, BIND=self.endpoints[i], NODE_ID=str(i))
            if i == self.proxy and hasattr(self, 'configure_proxy'):
                self.configure_proxy(env, config, ports)
            self.processes.append(subprocess.Popen([str(ROOT / 'bin' / ('ec_node' if i < self.proxy else 'ec_proxy'))],
                                                   env=env, stdout=log, stderr=log))
        for i in range(self.proxy + 1):
            deadline = time.monotonic() + 15
            while True:
                try:
                    self.request(i, 'PING', *([str(i)] if i < self.proxy else []))
                    break
                except (zmq.Again, AssertionError):
                    if time.monotonic() >= deadline or self.processes[i].poll() is not None:
                        log = self.logs[i]; log.flush(); log.seek(0)
                        self.fail(f'process {i} failed to start: {log.read()}')
                    time.sleep(.05)

    def stop(self):
        for p in self.processes:
            if p.poll() is None:
                os.kill(p.pid, signal.SIGCONT)
                p.terminate()
        for p in self.processes:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait()
        for log in self.logs:
            log.close()

    def request(self, node, *frames, ok=True):
        with self.context.socket(zmq.REQ) as s:
            s.linger = 0
            s.sndtimeo = s.rcvtimeo = 5000
            s.connect(self.endpoints[node])
            s.send_multipart([f if isinstance(f, bytes) else str(f).encode() for f in frames])
            reply = s.recv_multipart()
        self.assertEqual(len(reply), 2)
        if ok:
            self.assertEqual(reply[0], b'OK', reply)
            return reply[1]
        return reply

    def test_allocator_retries_and_ownership(self):
        owner = 'b' * 32
        def holes(sequence, op, body, ok=True):
            return self.request(0, 'HOLES', 0, owner, 0, sequence, op, body, ok=ok)
        holes(1, 'ADD', '99 256')
        holes(1, 'ADD', '99 256')  # lost ADD reply
        plan = holes(2, 'PLAN', '80 4 8')
        self.assertEqual(holes(3, 'COMMIT', plan), b'1')
        self.assertEqual(holes(3, 'COMMIT', plan), b'1')
        holes(4, 'RELEASE', '99 0 80')
        holes(4, 'RELEASE', '99 0 80')  # no double free
        self.assertEqual(holes(5, 'REMOVE', '99'), b'1')
        self.assertNotEqual(holes(4, 'RELEASE', '99 0 80', ok=False)[0], b'OK')
        self.assertNotEqual(self.request(0, 'HOLES', 0, 'c'*32, 0, 6, 'ADD', '100 256', ok=False)[0], b'OK')

    def test_parity_duplicate_and_abort_fence(self):
        name = 'a'*32 + '_2'
        self.request(2, 'WRITE', 2, name, bytes(256))
        transaction = 'c'*32 + '_1_0'
        payload = pack(pack(name, 256, pack(7, b'xyz')))
        self.request(2, 'TX_PATCH', 2, transaction, payload)
        self.request(2, 'TX_PATCH', 2, transaction, payload)
        self.assertEqual(self.request(2, 'READ', 2, name), bytes(7)+b'xyz'+bytes(246))
        self.request(2, 'TX_PARITY_ABORT', 2, transaction)
        self.request(2, 'TX_PARITY_ABORT', 2, transaction)
        self.assertEqual(self.request(2, 'READ', 2, name), bytes(256))
        self.assertNotEqual(self.request(2, 'TX_PATCH', 2, transaction, payload, ok=False)[0], b'OK')
        # An abort can overtake an apply after a timeout on another connection.
        next_id = 'c'*32 + '_2_0'
        self.request(2, 'TX_PARITY_ABORT', 2, next_id)
        self.assertNotEqual(self.request(2, 'TX_PATCH', 2, next_id, payload, ok=False)[0], b'OK')

    def test_updates_rollback_and_degraded_reads(self):
        old = b'a'*64
        self.request(self.proxy, 'PUT', 'key', old)
        metadata = json.loads(self.request(self.proxy, 'META', 'key'))
        # Initial placement of one key is rotation zero: data node 0, parity 2.
        self.assertEqual(self.request(self.proxy, 'GET', 'key'), old)
        for value in (b'b'*64, b'c'*80):
            os.kill(self.processes[self.proxy-1].pid, signal.SIGSTOP)
            reply = self.request(self.proxy, 'UPDATE', 'key', value, ok=False)
            self.assertNotEqual(reply[0], b'OK')
            stats = json.loads(self.request(self.proxy, 'STATS'))
            self.assertTrue(stats['pending_update'])
            self.assertNotEqual(self.request(self.proxy, 'GET', 'key', ok=False)[0], b'OK')
            os.kill(self.processes[self.proxy-1].pid, signal.SIGCONT)
            deadline = time.monotonic() + 10
            while True:
                reply = self.request(self.proxy, 'GET', 'key', ok=False)
                if reply[0] == b'OK':
                    self.assertEqual(reply[1], old)
                    break
                self.assertLess(time.monotonic(), deadline, reply)
            self.assertEqual(json.loads(self.request(self.proxy, 'META', 'key')), metadata)
            # After rollback, decoding with the data node unavailable must also
            # produce the old value; otherwise parity was left partially updated.
            os.kill(self.processes[0].pid, signal.SIGSTOP)
            try:
                self.assertEqual(self.request(self.proxy, 'GET', 'key'), old)
            finally:
                os.kill(self.processes[0].pid, signal.SIGCONT)
        self.request(self.proxy, 'UPDATE', 'key', b'd'*80)
        self.assertEqual(self.request(self.proxy, 'GET', 'key'), b'd'*80)
        self.request(self.proxy, 'DELETE', 'key')
        self.request(self.proxy, 'PUT', 'next', b'e'*80)
        self.assertEqual(self.request(self.proxy, 'GET', 'next'), b'e'*80)

    def test_data_replay_and_commit(self):
        prefix = 'e'*32
        for i in range(self.proxy):
            self.request(i, 'WRITE', i, prefix+f'_{i}', bytes(256))
        header = f'0 2 {self.PARITY} 256\n' + ''.join(f'{i} {prefix}_{i}\n' for i in range(2, self.proxy))
        payload = pack(pack(prefix+'_0', header, pack(9, b'hello')))
        tx = 'f'*32+'_1'
        self.request(0, 'TX_RMW', 0, tx, payload)
        before = [self.request(i, 'READ', i, prefix+f'_{i}') for i in range(self.proxy)]
        self.request(0, 'TX_RMW', 0, tx, payload)
        self.assertEqual(before, [self.request(i, 'READ', i, prefix+f'_{i}') for i in range(self.proxy)])
        self.request(0, 'TX_COMMIT', 0, tx)
        self.request(0, 'TX_COMMIT', 0, tx)
        self.request(0, 'TX_ABORT', 0, 'f'*32+'_2')
        self.assertNotEqual(self.request(0, 'TX_RMW', 0, tx, payload, ok=False)[0], b'OK')
        self.assertEqual(before, [self.request(i, 'READ', i, prefix+f'_{i}') for i in range(self.proxy)])

    def test_unified_placement_and_equal_size_map(self):
        self.request(self.proxy, 'PUT', 'key', b'a'*60)
        original = json.loads(self.request(self.proxy, 'META', 'key'))
        self.request(self.proxy, 'UPDATE', 'key', b'b'*60)
        self.assertEqual(json.loads(self.request(self.proxy, 'META', 'key')), original)
        for length in (61, 400, 100, 600, 50):
            before = json.loads(self.request(self.proxy, 'STATS'))
            loads = [d['cumulative_bytes']+p['cumulative_bytes']
                     for d, p in zip(before['data_load'], before['parity_load'])]
            self.request(self.proxy, 'UPDATE', 'key', b'c'*length)
            after = json.loads(self.request(self.proxy, 'STATS'))
            selection = after['last_placement']
            self.assertEqual(selection['load_bytes_at_selection'], loads)
            self.assertEqual(selection['data_bytes_at_selection'], loads)
            self.assertEqual(selection['parity_cumulative_bytes'], sum(loads[n] for n in selection['parity_nodes']))
            self.assertEqual(after['rebalance_events'], after['resize_updates'])
            self.assertEqual(self.request(self.proxy, 'GET', 'key'), b'c'*length)

    def test_lazy_chunks_random_resizes_and_reconstruction(self):
        self.request(self.proxy, 'PUT', 'first', b'x'*60)
        first = json.loads(self.request(self.proxy, 'STATS'))
        self.assertEqual(first['reserved_data_bytes'], 256)
        self.assertEqual(first['active_parity_bytes'], 256*self.PARITY)
        self.assertEqual(first['virtual_zero_data_chunks'], 1)
        rng = random.Random(111)
        values = {'first': b'x'*60}
        for n in range(12):
            key = f'key{n}'; value = rng.randbytes(rng.randrange(20, 700))
            self.request(self.proxy, 'PUT', key, value); values[key] = value
        for _ in range(60):
            key = rng.choice(list(values)); value = rng.randbytes(rng.randrange(0, 700))
            self.request(self.proxy, 'UPDATE', key, value); values[key] = value
        state = json.loads(self.request(self.proxy, 'STATS'))
        self.assertEqual(state['logical_live_bytes'], sum(map(len, values.values())))
        self.assertEqual(state['logical_live_bytes'] + state['free_data_bytes'], state['reserved_data_bytes'])
        actual = sum(n['parity']['memory']['groups']['blocks']['value_bytes']
                     for n in state['nodes'] if 'blocks' in n['parity']['memory']['groups'])
        self.assertEqual(actual, state['space_accounted_payload_bytes'])
        for key, value in values.items():
            self.assertEqual(self.request(self.proxy, 'GET', key), value)
        os.kill(self.processes[0].pid, signal.SIGSTOP)
        try:
            for key, value in values.items():
                self.assertEqual(self.request(self.proxy, 'GET', key), value)
        finally:
            os.kill(self.processes[0].pid, signal.SIGCONT)


class TwoParityCluster(NativeCluster):
    PARITY = 2


if __name__ == '__main__':
    unittest.main()
