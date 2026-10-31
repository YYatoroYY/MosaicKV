"""Measurement regressions using synthetic counters/services, never paper data."""
import csv
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'benchmarks'), str(ROOT / 'experiments')]
from collect_service_cpu import read_counters
from cpu_accounting import cpu_delta
from remote_cpu import RemoteCpuMeter
from space_trace import SpaceTrace
import run_endpoint


def state(values=None, scheme='fastgc', phase='resize'):
    values = values or {}
    logical = sum(map(len, values.values()))
    return dict(keys=len(values), k=2, m=1, update_strategy=scheme, workload_phase=phase,
                pending_update=False, nodes=[dict(id=i, healthy=True, parity=dict(
                    memory={}, traffic=dict(enabled=True, received_bytes=0, sent_bytes=0))) for i in range(3)],
                node_requests=dict(traffic_enabled=True), gc=dict(logical_live_bytes=logical,
                    total_data_bytes=logical, invalid_data_bytes=0, live_padding_bytes=0,
                    active_parity_bytes=logical, retired_ec_catalog_bytes=0, phase='idle',
                    active_node_payload_bytes=logical * 2))


def counters(value, identity='original'):
    return dict(container_id='container', cgroup_cpu_stat='/cpu', identity=identity,
                usage_usec=value, user_usec=value * 3 // 4, system_usec=value // 4)


class CpuTests(unittest.TestCase):
    def test_v2_and_v1_units(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'docker/one').mkdir(parents=True)
            (root/'docker/one/cpu.stat').write_text('usage_usec 40000\nuser_usec 30000\nsystem_usec 10000\n')
            _, values = read_counters('0::/docker/one', root)
            self.assertEqual(values['usage_usec'], 40000)
            (root/'cpu,cpuacct/docker/one').mkdir(parents=True)
            (root/'cpu,cpuacct/docker/one/cpuacct.usage').write_text('40000000')
            (root/'cpu,cpuacct/docker/one/cpuacct.stat').write_text('user 3\nsystem 1\n')
            with patch('collect_service_cpu.os.sysconf', return_value=100, create=True):
                _, legacy = read_counters('2:cpu,cpuacct:/docker/one', root)
            self.assertEqual(legacy, values)

    def test_totals_and_restart_detection(self):
        before = dict(start_monotonic_ns=0, end_monotonic_ns=10, services={
            'proxy': counters(0), 'node0': counters(0), 'node1': counters(0)})
        after = dict(start_monotonic_ns=1000000000, end_monotonic_ns=1000000010, services={
            'proxy': counters(1000000), 'node0': counters(2000000), 'node1': counters(3000000)})
        result = cpu_delta(before, after)
        self.assertEqual((result['proxy_seconds'], result['storage_seconds'], result['total_seconds']), (1, 5, 6))
        after['services']['node1']['identity'] = 'restarted'
        with self.assertRaisesRegex(ValueError, 'identity'):
            cpu_delta(before, after)

    def test_inventory_coverage_and_duplicate_container(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'hosts.json'
            services = {'proxy': dict(host='local', container='proxy')}
            path.write_text(json.dumps(dict(services=services)))
            with self.assertRaisesRegex(ValueError, 'exactly'):
                RemoteCpuMeter(path, [0])
            services['node0'] = dict(host='local', container='proxy')
            path.write_text(json.dumps(dict(services=services)))
            with self.assertRaisesRegex(ValueError, 'more than once'):
                RemoteCpuMeter(path, [0])

    def test_host_failure_and_partial_snapshot_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'hosts.json'
            path.write_text(json.dumps(dict(services={'proxy': dict(host='bench@proxy', container='proxy'),
                                                      'node0': dict(host='bench@node', container='node')})))
            meter = RemoteCpuMeter(path, [0])
            with patch('remote_cpu.subprocess.run', side_effect=subprocess.TimeoutExpired('ssh', 1)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    meter.snapshot()
            with patch('remote_cpu.subprocess.run', return_value=SimpleNamespace(stdout='{}')):
                with self.assertRaisesRegex(ValueError, 'Incomplete'):
                    meter.snapshot()

    def test_remote_snapshot_protocol_and_alias_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'hosts.json'
            path.write_text(json.dumps(dict(services={'proxy': dict(host='bench@proxy', container='proxy'),
                                                      'node0': dict(host='bench@node', container='node')})))
            meter = RemoteCpuMeter(path, [0])
            def reply(command, **kwargs):
                self.assertEqual(command[0], 'ssh')
                self.assertIn('BatchMode=yes', command)
                self.assertIn('def collect(services)', kwargs['input'])
                host = command[-2]
                name = 'proxy' if host == 'bench@proxy' else 'node0'
                return SimpleNamespace(stdout=json.dumps({name: counters(100, ['boot', name, 1, 'started'])}))
            with patch('remote_cpu.subprocess.run', side_effect=reply):
                result = meter.snapshot()
            self.assertEqual(set(result['services']), {'proxy', 'node0'})
            self.assertEqual(result['services']['proxy']['host'], 'bench@proxy')
            def duplicate(item):
                host, group = item
                return {name: dict(identity=[host, 'boot', 'same-container', 1, 'started']) for name in group}
            with patch.object(meter, '_read_host', side_effect=duplicate):
                with self.assertRaisesRegex(ValueError, 'same container'):
                    meter.snapshot()


class SpaceTests(unittest.TestCase):
    def test_boundaries_normalization_and_plot(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            trace = SpaceTrace(path, 'fastgc', 2, 5)
            self.assertEqual([i for i in range(1, 6) if trace.due(i)], [2, 4])
            for count, stage in [(0, 'initial'), (2, 'update'), (4, 'update'), (5, 'foreground_end'), (5, 'post_gc')]:
                trace.sample(count, stage, lambda: state({'key': b'x' * (count + 1)}))
            self.assertEqual([row['completed_operations'] for row in trace.rows], [0, 2, 4, 5, 5])
            self.assertAlmostEqual(trace.rows[-1]['space_amplification'], 4/3)
            self.assertEqual(len((path/'space-trace-stats.jsonl').read_text().splitlines()), 5)
            (path/'manifest.json').write_text('{"passed": true}')
            from plot_space_trace import plot_traces
            plot_traces([path], path)
            self.assertGreater((path/'space-amplification-progress.png').stat().st_size, 1000)
            self.assertGreater((path/'space-amplification-progress.pdf').stat().st_size, 1000)
            (path/'manifest.json').write_text('{"passed": false}')
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                plot_traces([path], path)

    def test_incomplete_or_pending_snapshot_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            trace = SpaceTrace(tmp, 'fastgc', 2, 5)
            broken = state({'key': b'x'})
            broken['nodes'][0]['healthy'] = False
            with self.assertRaises(RuntimeError):
                trace.sample(0, 'initial', lambda: broken)
            broken = state({'key': b'x'})
            broken['pending_update'] = True
            with self.assertRaises(AssertionError):
                trace.sample(0, 'initial', lambda: broken)
            self.assertFalse((Path(tmp)/'space-trace.csv').exists())


class FakeClient:
    def __init__(self, *args):
        self.values, self.phase, self.cpu, self.events = {}, 'load', 0, []

    def request(self, op, *args):
        self.events.append(op)
        self.cpu += 7 if op == 'GC' else 10
        if op in ('PUT', 'UPDATE'):
            self.values[args[0]] = args[1]
            return b''
        if op == 'GET':
            return self.values[args[0]]
        if op == 'PHASE':
            self.phase = 'resize'
            return b''
        if op == 'META':
            return json.dumps(dict(extents=[dict(storage='ec', block_index=0, shards=[dict(node=0)])])).encode()
        if op == 'STATS':
            return json.dumps(state(self.values, phase=self.phase)).encode()
        if op == 'GC':
            return b'{}'
        if op == 'REPAIR_BENCH':
            return b'1'
        raise AssertionError(op)

    def close(self):
        pass


class FakeMeter:
    def __init__(self, client):
        self.client, self.clock, self.services = client, 0, {}

    def snapshot(self):
        self.client.events.append('CPU')
        self.clock += 1000000000
        return dict(start_monotonic_ns=self.clock, end_monotonic_ns=self.clock+1,
                    services={name: counters(self.client.cpu * 1000) for name in ['proxy', 'node0', 'node1', 'node2']})


class EndpointTests(unittest.TestCase):
    def run_case(self, root, experiment='update', sampling=False, cpu=True):
        path = root/'workload.json'
        path.write_text(json.dumps(dict(initial_lengths={'key': 1}, updates=[
            dict(key='key', old_bytes=i, new_bytes=i+1) for i in range(1, 4)])))
        output = root/'result'
        client = FakeClient()
        args = ['run_endpoint.py', '--endpoint', 'tcp://unused:5555', '--workload-file', str(path),
                '--experiment', experiment, '--reads', '4', '--repair-count', '2', '--output', str(output)]
        if cpu:
            args += ['--cpu-hosts', 'unused.json']
        if sampling:
            args += ['--space-sample-every', '2']
        with patch.object(sys, 'argv', args), patch.object(run_endpoint, 'Client', return_value=client), \
             patch.object(run_endpoint, 'RemoteCpuMeter', return_value=FakeMeter(client)), \
             patch.object(run_endpoint, 'audit_links'), patch.object(run_endpoint, 'endpoint_totals', return_value={}), \
             patch.object(run_endpoint, 'balance', return_value={}), patch('sys.stdout', new_callable=io.StringIO):
            run_endpoint.main()
        return output, client

    def test_update_cpu_includes_gc_but_not_preload_or_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, client = self.run_case(Path(tmp))
            metrics = json.loads((output/'metrics.json').read_text())
            self.assertAlmostEqual(metrics['cpu_proxy_seconds'], .037)
            self.assertAlmostEqual(metrics['cpu_storage_seconds'], .111)
            self.assertAlmostEqual(metrics['cpu_gc_tail_seconds'], .028)
            self.assertAlmostEqual(metrics['cpu_ms_per_update'], 148/3)
            cpu = json.loads((output/'cpu-accounting.json').read_text())
            self.assertAlmostEqual(cpu['foreground']['total_seconds'], .120)
            self.assertEqual(client.events[-1], 'META')
            self.assertTrue(json.loads((output/'manifest.json').read_text())['passed'])

    def test_read_cpu_excludes_update_preparation(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, _ = self.run_case(Path(tmp), experiment='read')
            metrics = json.loads((output/'metrics.json').read_text())
            self.assertAlmostEqual(metrics['cpu_ms_per_read'], 40)
            self.assertEqual(metrics['cpu_gc_tail_seconds'], 0)

    def test_endpoint_space_progress_and_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, _ = self.run_case(Path(tmp), sampling=True, cpu=False)
            with (output/'space-trace.csv').open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual([r['completed_operations'] for r in rows], ['0', '2', '3', '3'])
            self.assertEqual(rows[-1]['stage'], 'post_gc')
            self.assertFalse((output/'cpu-accounting.json').exists())

    def test_cpu_and_sampling_are_separate(self):
        with tempfile.TemporaryDirectory() as tmp, patch('sys.stderr', new_callable=io.StringIO):
            with self.assertRaises(SystemExit):
                self.run_case(Path(tmp), sampling=True, cpu=True)

    def test_cpu_failure_leaves_run_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = FakeMeter.snapshot
            def fail_after_start(meter):
                if meter.clock >= 2000000000:
                    raise RuntimeError('host lost')
                return original(meter)
            with patch.object(FakeMeter, 'snapshot', fail_after_start):
                with self.assertRaisesRegex(RuntimeError, 'host lost'):
                    self.run_case(Path(tmp))
            self.assertFalse(json.loads((Path(tmp)/'result/manifest.json').read_text())['passed'])

    def test_local_worker_samples_between_acknowledged_updates(self):
        import zmq
        from small_object_resize import measure
        with tempfile.TemporaryDirectory() as tmp:
            context = zmq.Context()
            server = context.socket(zmq.REP)
            port = server.bind_to_random_port('tcp://127.0.0.1')
            values = {'key': b'x'}
            stop = threading.Event()
            def serve():
                while not stop.is_set():
                    if server.poll(100):
                        _, key, value = server.recv_multipart()
                        values[key.decode()] = value
                        server.send_multipart([b'OK', b''])
            thread = threading.Thread(target=serve)
            thread.start()
            cluster = SimpleNamespace(context=context, port=port,
                                      request=lambda op: json.dumps(state(values)).encode())
            try:
                args = SimpleNamespace(warmup=0, operations=5, space_sample_every=2)
                result = measure(cluster, args, 1, [('key', i, b'x'*(i+1)) for i in range(1, 6)],
                                 checkpoint_directory=Path(tmp))
                self.assertEqual(result['metrics']['failed_operations'], 0)
                with (Path(tmp)/'space-trace.csv').open() as stream:
                    rows = list(csv.DictReader(stream))
                self.assertEqual([r['completed_operations'] for r in rows], ['0', '2', '4', '5'])
                self.assertEqual([int(r['logical_live_bytes']) for r in rows], [1, 3, 5, 6])
            finally:
                stop.set()
                thread.join(timeout=3)
                server.close()
                context.term()


if __name__ == '__main__':
    unittest.main()
