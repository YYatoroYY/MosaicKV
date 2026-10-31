#!/usr/bin/env python3
"""Replay a workload against an empty, already deployed MosaicKV cluster."""
import argparse
from contextlib import contextmanager
import csv
import hashlib
import json
from pathlib import Path
import sys
import time

import numpy as np
import zmq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'benchmarks'))
from space_overhead import snapshot as space_snapshot
from update_traffic_compare import balance, endpoint_totals, audit_links
from metadata_accounting import snapshot as metadata_snapshot
from stats_snapshot import require_node_stats
from prepare_sharegpt import iter_initial, iter_updates, file_hash
from cpu_accounting import cpu_delta
from remote_cpu import RemoteCpuMeter, REMOTE_CPU_SCOPE
from space_trace import SpaceTrace


class Client:
    def __init__(self, endpoint, timeout_ms):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.linger = 0
        self.socket.sndtimeo = self.socket.rcvtimeo = timeout_ms
        self.socket.connect(endpoint)

    def request(self, op, *args):
        self.socket.send_multipart([op.encode(), *(a.encode() if isinstance(a, str) else a for a in args)])
        reply = self.socket.recv_multipart()
        if len(reply) != 2 or reply[0] != b'OK':
            raise RuntimeError(f'{op}: {reply!r}')
        return reply[1]

    def close(self):
        self.socket.close()
        self.context.term()

    @contextmanager
    def persistent_requests(self, timeout_ms):
        old = self.socket.rcvtimeo
        self.socket.sndtimeo = self.socket.rcvtimeo = timeout_ms
        try:
            yield self
        finally:
            self.socket.sndtimeo = self.socket.rcvtimeo = old


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def workload(args):
    if args.sharegpt_bundle:
        bundle = args.sharegpt_bundle
        manifest = json.loads((bundle / 'manifest.json').read_text())
        for name, entry in manifest['files'].items():
            if file_hash(bundle / name) != entry['sha256']:
                raise ValueError(f'ShareGPT bundle checksum mismatch: {name}')
        initial = dict(iter_initial(bundle))
        expected = json.loads((bundle / 'expected_final.json').read_text())
        def updates():
            yield from iter_updates(bundle, full_update=args.update_mode == 'full')
        return initial, updates, expected, dict(kind='sharegpt', manifest=manifest, operations=manifest['updates'])
    raw = args.workload_file.read_bytes()
    data = json.loads(raw)
    if any(not isinstance(size, int) or not 1 <= size <= 4096 or not 0 < len(key.encode()) <= 128
           for key, size in data['initial_lengths'].items()):
        raise ValueError('invalid initial key or value length')
    initial = {key: bytes([65 + i % 26]) * size
               for i, (key, size) in enumerate(data['initial_lengths'].items())}
    def updates():
        current = initial.copy()
        for step in data['updates']:
            key, new = step['key'], step['new_bytes']
            old = current[key]
            if len(old) != step['old_bytes'] or not 1 <= new <= 4096:
                raise ValueError('invalid workload update length')
            value = (b'\xa5' if old[:1] != b'\xa5' else b'\x5a') * new
            current[key] = value
            yield key, value, 'UPDATE', len(old), new
    final = initial.copy()
    for key, value, _, _, _ in updates():
        final[key] = value
    expected = {key: dict(bytes=len(value), sha256=hashlib.sha256(value).hexdigest())
                for key, value in final.items()}
    return initial, updates, expected, dict(kind='ycsb', operations=len(data['updates']), sha256=hashlib.sha256(raw).hexdigest(),
                                          metadata=data.get('metadata', {}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True, help='tcp://PROXY_IP:5555')
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--workload-file', type=Path)
    source.add_argument('--sharegpt-bundle', type=Path)
    p.add_argument('--experiment', choices=['update', 'read', 'repair'], default='update')
    p.add_argument('--update-mode', choices=['full', 'append'], default='full')
    p.add_argument('--reads', type=int, default=100000)
    p.add_argument('--repair-count', type=int, default=1000)
    p.add_argument('--zipf-theta', type=float, default=.99)
    p.add_argument('--seed', type=int, default=20261018)
    p.add_argument('--timeout-ms', type=int, default=60000)
    p.add_argument('--record-metadata-memory', action='store_true')
    p.add_argument('--cpu-hosts', type=Path, help='JSON service-to-SSH-host/container inventory for CPU accounting')
    p.add_argument('--cpu-timeout', type=int, default=30, help='per-host CPU collection timeout in seconds')
    p.add_argument('--space-sample-every', type=int, default=0,
                   help='sample space after every N completed updates; 0 disables instrumented tracing')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.space_sample_every < 0 or args.cpu_timeout <= 0:
        p.error('space sample interval must be nonnegative and CPU timeout positive')
    if args.space_sample_every and args.experiment != 'update':
        p.error('space tracing requires --experiment update')
    if args.space_sample_every and args.cpu_hosts:
        p.error('collect CPU and space curves in separate replays to exclude sampling work from CPU cost')
    if min(args.reads, args.repair_count, args.timeout_ms) <= 0 or not np.isfinite(args.zipf_theta) or args.zipf_theta < 0:
        p.error('counts and timeout must be positive; Zipf theta must be finite and nonnegative')
    if args.update_mode == 'append' and not args.sharegpt_bundle:
        p.error('--update-mode append requires --sharegpt-bundle')
    if args.output.exists():
        p.error('choose a new output directory')
    initial, updates, expected, source_info = workload(args)
    if not initial:
        p.error('empty workload')
    args.output.mkdir(parents=True)
    manifest = dict(passed=False, endpoint=args.endpoint, experiment=args.experiment,
                    source=source_info, update_mode=args.update_mode,
                    seed=args.seed, zipf_theta=args.zipf_theta,
                    space_sample_every=args.space_sample_every, cpu_enabled=bool(args.cpu_hosts))
    write_json(args.output / 'manifest.json', manifest)
    client = Client(args.endpoint, args.timeout_ms)
    try:
        ready = json.loads(client.request('STATS'))
        require_node_stats(ready, 'ready')
        if ready['keys'] or ready['pending_update'] or ready['workload_phase'] != 'load':
            raise ValueError('use a fresh empty cluster started with EC_WORKLOAD_PHASE=load')
        scheme, k, m = ready['update_strategy'], ready['k'], ready['m']
        cpu_meter = RemoteCpuMeter(args.cpu_hosts, [n['id'] for n in ready['nodes']], args.cpu_timeout) if args.cpu_hosts else None
        if cpu_meter:
            cpu_meter.snapshot()  # Fail on missing hosts/permissions before modifying the cluster.
            manifest['cpu_services'] = cpu_meter.services
        if args.update_mode == 'append' and scheme != 'mosaic':
            raise ValueError('APPEND mode is supported by MosaicKV; use full UPDATE for cross-scheme comparisons')
        for key, value in initial.items():
            client.request('PUT', key, value)
        padding = 0
        while (len(initial) + padding) % k:
            key = f'__endpoint_padding__:{padding}'
            if key in initial:
                raise ValueError('padding key collision')
            client.request('PUT', key, b'P')
            padding += 1
        for key, value in initial.items():
            if client.request('GET', key) != value:
                raise ValueError('preload readback mismatch')
        loaded = json.loads(client.request('STATS'))
        require_node_stats(loaded, 'preload')
        write_json(args.output / 'preload.json', dict(keys=len(initial), padding_keys=padding, stats=loaded))
        client.request('PHASE', 'RESIZE')
        samples = []
        def timed(op, key, *payload):
            start = time.perf_counter_ns()
            result = client.request(op, key, *payload)
            elapsed = time.perf_counter_ns() - start
            samples.append(dict(sequence=len(samples), operation=op, key=key, latency_ns=elapsed))
            return result
        if args.experiment != 'update':
            for key, payload, op, _, _ in updates():
                client.request(op, key, payload)
            if scheme == 'fastgc':
                client.request('GC', 'QUIESCE')
        # Prepare selection and repair metadata before the CPU measurement window.
        if args.experiment == 'read':
            keys = list(initial)
            weights = np.arange(1, len(keys) + 1, dtype=float) ** -args.zipf_theta
            weights /= weights.sum()
            indices = np.random.default_rng(args.seed).choice(len(keys), args.reads, p=weights)
        elif args.experiment == 'repair':
            candidates = []
            for key in initial:
                meta = json.loads(client.request('META', key))
                if meta['extents'] and all(e['storage'] == 'ec' for e in meta['extents']):
                    extent = meta['extents'][0]
                    candidates.append((key, extent['shards'][extent['block_index']]['node']))
            if not candidates:
                raise ValueError('repair requires encoded objects')
            manifest['repair_eligible_keys'] = len(candidates)
            indices = np.random.default_rng(args.seed).integers(len(candidates), size=args.repair_count)
        before = json.loads(client.request('STATS'))
        require_node_stats(before, 'before')
        if not before['node_requests']['traffic_enabled'] or not all(
                node['parity']['traffic']['enabled'] for node in before['nodes']):
            raise ValueError('start proxy and memory services with EC_TRAFFIC=1')
        write_json(args.output / 'stats-before.json', before)
        space_trace = (SpaceTrace(args.output, scheme, args.space_sample_every, source_info['operations'])
                       if args.space_sample_every else None)
        if space_trace:
            space_trace.sample(0, 'initial', lambda: before)
        cpu_before = cpu_meter.snapshot() if cpu_meter else None
        if args.experiment == 'update':
            for completed, (key, payload, op, _, _) in enumerate(updates(), 1):
                timed(op, key, payload)
                if space_trace and space_trace.due(completed):
                    space_trace.sample(completed, 'update', lambda: json.loads(client.request('STATS')))
            if len(samples) != source_info['operations']:
                raise ValueError('Update count differs from workload metadata')
        elif args.experiment == 'read':
            for index in indices:
                key = keys[index]
                value = timed('GET', key)
                if hashlib.sha256(value).hexdigest() != expected[key]['sha256']:
                    raise ValueError('timed GET readback mismatch')
        else:
            # Each request reconstructs an encoded object's data on a reachable
            # target, excluding that target as a source. No failure-detection timer.
            for index in indices:
                key, node = candidates[index]
                if int(timed('REPAIR_BENCH', key, str(node))) <= 0:
                    raise ValueError('repair benchmark did not reconstruct any fragments')
        cpu_foreground = cpu_meter.snapshot() if cpu_meter else None
        if space_trace:
            space_trace.sample(len(samples), 'foreground_end', lambda: json.loads(client.request('STATS')))
        tail = 0.0
        if scheme == 'fastgc' and args.experiment == 'update':
            foreground_end = time.perf_counter()
            client.request('GC', 'QUIESCE')
            tail = time.perf_counter() - foreground_end
        cpu_after = (cpu_meter.snapshot() if scheme == 'fastgc' and args.experiment == 'update'
                     else cpu_foreground) if cpu_meter else None
        if cpu_meter:
            total = cpu_delta(cpu_before, cpu_after)
            foreground = cpu_delta(cpu_before, cpu_foreground)
            gc_tail = cpu_delta(cpu_foreground, cpu_after)
            write_json(args.output / 'cpu-accounting.json', dict(scope=REMOTE_CPU_SCOPE,
                       before=cpu_before, foreground_end=cpu_foreground, after=cpu_after,
                       total=total, foreground=foreground, gc_tail=gc_tail))
        after = json.loads(client.request('STATS'))
        require_node_stats(after, 'after')
        write_json(args.output / 'stats-after.json', after)
        if space_trace and scheme == 'fastgc':
            space_trace.sample(len(samples), 'post_gc', lambda: after)
        result = dict(before=before, after=after)
        audit_links(result)
        traffic = endpoint_totals(result)
        node_bytes = [b['parity']['traffic']['received_bytes'] + b['parity']['traffic']['sent_bytes']
                      - a['parity']['traffic']['received_bytes'] - a['parity']['traffic']['sent_bytes']
                      for a, b in zip(before['nodes'], after['nodes'])]
        fragments = []
        for key, expected_value in expected.items():
            value = client.request('GET', key)
            if len(value) != expected_value['bytes'] or hashlib.sha256(value).hexdigest() != expected_value['sha256']:
                raise ValueError('final readback mismatch')
            fragments.append(len(json.loads(client.request('META', key))['extents']))
        if args.record_metadata_memory:
            write_json(args.output / 'metadata-memory.json', metadata_snapshot(client, len(initial) + padding, 'final'))
        latency = np.array([s['latency_ns'] for s in samples], dtype=float) / 1e6
        if not len(latency):
            raise ValueError('no measured operations')
        metrics = dict(scheme=scheme, k=k, m=m, experiment=args.experiment, operations=len(samples),
                       mean_ms=float(latency.mean()), p50_ms=float(np.percentile(latency, 50)),
                       p99_ms=float(np.percentile(latency, 99)),
                       rpc_service_ops_per_second=1000 * len(latency) / float(latency.sum()),
                       gc_tail_seconds=tail, mean_fragments=float(np.mean(fragments)),
                       **traffic, **balance(node_bytes), **space_snapshot(after, scheme))
        metrics.update(space_sample_every=args.space_sample_every,
                       instrumented_space_run=bool(space_trace))
        if cpu_meter:
            metrics.update(cpu_total_seconds=total['total_seconds'], cpu_proxy_seconds=total['proxy_seconds'],
                           cpu_storage_seconds=total['storage_seconds'], cpu_user_seconds=total['user_seconds'],
                           cpu_system_seconds=total['system_seconds'],
                           cpu_ms_per_operation=1000 * total['total_seconds'] / len(samples),
                           cpu_proxy_ms_per_operation=1000 * total['proxy_seconds'] / len(samples),
                           cpu_storage_ms_per_operation=1000 * total['storage_seconds'] / len(samples),
                           cpu_foreground_seconds=foreground['total_seconds'], cpu_gc_tail_seconds=gc_tail['total_seconds'])
            metrics['cpu_ms_per_' + args.experiment] = metrics['cpu_ms_per_operation']
        write_json(args.output / 'metrics.json', metrics)
        for filename, rows in [('summary.csv', [metrics]), ('latency.csv', samples),
                               ('per-node.csv', [dict(node=n['id'], total_bytes=v) for n, v in zip(after['nodes'], node_bytes)])]:
            with (args.output / filename).open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        manifest.update(passed=True, keys_checked=len(expected), padding_keys=padding,
                        service_configuration=ready, measurement='client RPC latency; preparation and readback checks excluded',
                        repair_mode='forced reconstruction to a reachable target' if args.experiment == 'repair' else None)
        write_json(args.output / 'manifest.json', manifest)
        print(json.dumps(metrics, indent=2))
    finally:
        client.close()


if __name__ == '__main__':
    main()
