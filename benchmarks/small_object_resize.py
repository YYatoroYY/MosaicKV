from storage_backend import validate_storage

import argparse

from array import array

from collections import Counter

from concurrent.futures import ThreadPoolExecutor

import csv

from datetime import datetime, timezone

import hashlib

import json

import os

from pathlib import Path

import random

import subprocess

import sys

import tempfile

import threading

import time

import zmq

from cluster import ROOT, STRATEGIES, DirectCluster, percentile, process_io

from stats_snapshot import read_stats
from space_trace import SpaceTrace

def measure(cluster, args, clients, traces, settle_gc=False, collect_latency=True, checkpoint_directory=None, cpu_meter=None):
    space_every = getattr(args, 'space_sample_every', 0)
    if space_every < 0 or (space_every and (clients != 1 or checkpoint_directory is None)):
        raise ValueError('Space tracing requires one client and a checkpoint directory')
    space_trace = None
    owners = {key: i % clients for i, key in enumerate(dict.fromkeys(t[0] for t in traces))}
    warmed = threading.Barrier(clients + 1, timeout=180)
    timing = {}
    start = threading.Barrier(clients + 1, timeout=180,
                              action=lambda: timing.update(start=time.perf_counter()))

    def worker(index):
        # Build requests and reserve timestamp slots before either barrier.
        selected = [(i, old, len(value), [b'UPDATE', key.encode(), value])
                    for i, (key, old, value) in enumerate(traces)
                    if owners[key] == index]
        begins = array('Q', [0]) * len(selected) if collect_latency else None
        ends = array('Q', [0]) * len(selected) if collect_latency else None
        failures = bytearray(len(selected))
        measuring = False
        try:
            with cluster.context.socket(zmq.REQ) as sock:
                sock.linger = 0
                sock.sndtimeo = sock.rcvtimeo = 120000
                sock.connect(f'tcp://127.0.0.1:{cluster.port}')
                for slot, (sequence, old, length, request) in enumerate(selected):
                    if sequence >= args.warmup and not measuring:
                        warmed.wait()
                        start.wait()
                        measuring = True
                    if measuring and collect_latency:
                        begin = time.perf_counter_ns()
                    sock.send_multipart(request)
                    reply = sock.recv_multipart()
                    if measuring and collect_latency:
                        end = time.perf_counter_ns()
                    # Application failures are complete replies: count them and
                    # continue. Never replay an update after an unknown outcome.
                    failures[slot] = int(len(reply) != 2 or reply[0] != b'OK')
                    if measuring and collect_latency:
                        begins[slot], ends[slot] = begin, end
                    completed = sequence - args.warmup + 1
                    if measuring and space_trace and space_trace.due(completed):
                        if failures[slot]:
                            raise ValueError('Cannot sample a failed update replay')
                        space_trace.sample(completed, 'update', lambda: read_stats(cluster, 'space-update'))
                return selected, begins, ends, time.perf_counter(), failures
        except BaseException:
            warmed.abort()
            start.abort()
            raise

    with ThreadPoolExecutor(max_workers=clients) as executor:
        tasks = [executor.submit(worker, i) for i in range(clients)]
        try:
            warmed.wait()
            before = read_stats(cluster, 'before', checkpoint_directory)
            if space_every:
                space_trace = SpaceTrace(checkpoint_directory, before['update_strategy'], space_every, args.operations)
                space_trace.sample(0, 'initial', lambda: before)
            cpu_before = cpu_meter.snapshot() if cpu_meter else None
            start.wait()
            results = [task.result() for task in tasks]
        except BaseException:
            warmed.abort()
            start.abort()
            raise
    cpu_foreground = cpu_meter.snapshot() if cpu_meter else None
    if space_trace:
        space_trace.sample(args.operations, 'foreground_end', lambda: read_stats(cluster, 'space-foreground'))
    tail_start = time.perf_counter()
    checkpoint_seconds = 0
    if checkpoint_directory is not None:
        # Persist raw foreground samples before a potentially long GC barrier.
        # No sorting or checkpoint I/O occurs inside the timed UPDATE loop.
        checkpoint = dict(format='native_uint64_arrays', byteorder=sys.byteorder,
                          itemsize=array('Q').itemsize, warmup=args.warmup, operations=args.operations,
                          latency_collected=collect_latency, start=timing['start'], workers=[],
                          gc_tail_complete=False, before=before,
                          cpu_before=cpu_before, cpu_foreground=cpu_foreground)
        for index, (selected, begins, ends, finished, failures) in enumerate(results):
            stem = f'foreground-client{index}'
            sequences = array('Q', (item[0] for item in selected))
            arrays = dict(sequence=sequences)
            if collect_latency:
                arrays.update(begin_ns=begins, end_ns=ends)
            files = {}
            for name, values in arrays.items():
                path = checkpoint_directory / f'{stem}-{name}.bin'
                with path.open('wb') as file:
                    values.tofile(file)
                files[name] = path.name
            path = checkpoint_directory / f'{stem}-failures.bin'
            path.write_bytes(failures)
            files['failures'] = path.name
            checkpoint['workers'].append(dict(count=len(selected), finished=finished, files=files))
        checkpoint['write_seconds'] = time.perf_counter() - tail_start
        temporary = checkpoint_directory / 'foreground-checkpoint.json.tmp'
        temporary.write_text(json.dumps(checkpoint, indent=2))
        temporary.replace(checkpoint_directory / 'foreground-checkpoint.json')
        checkpoint_seconds = time.perf_counter() - tail_start
        print(f'FOREGROUND_COMPLETE: {args.operations} updates; raw latency samples saved', flush=True)
    if settle_gc:
        # Drain work already started, including retired/orphan shard cleanup;
        # do not force compaction below the configured trigger thresholds.
        timeout_ms = int(getattr(args, 'gc_tail_timeout_seconds', 30) * 1000)
        print(f'GC_TAIL_START: waiting for QUIESCE; timeout={timeout_ms / 1000:g}s', flush=True)
        with cluster.persistent_requests(timeout_ms=timeout_ms):
            state = json.loads(cluster.request('GC', 'QUIESCE'))
        assert state['suspended'] and not state['busy']
        print(f'GC_TAIL_COMPLETE: {time.perf_counter() - tail_start:.1f}s', flush=True)
    cpu_after = (cpu_meter.snapshot() if settle_gc else cpu_foreground) if cpu_meter else None
    tail_seconds = time.perf_counter() - tail_start if settle_gc else 0
    after = read_stats(cluster, 'after', checkpoint_directory)
    if space_trace and settle_gc:
        space_trace.sample(args.operations, 'post_gc', lambda: after)
    traffic_seconds = time.perf_counter() - timing['start']
    # No per-request dictionaries, unit conversion, sorting or percentiles in
    # the measured loop. Format records only after every client has finished.
    samples = []
    if collect_latency:
        for selected, begins, ends, _, failures in results:
            for slot, (sequence, old, length, _) in enumerate(selected):
                if sequence >= args.warmup:
                    samples.append(dict(sequence=sequence, old_bytes=old, new_bytes=length,
                                        latency_ms=(ends[slot] - begins[slot]) / 1e6,
                                        failed=bool(failures[slot])))
        samples.sort(key=lambda sample: sample['sequence'])
        assert len(samples) == args.operations
    seconds = max(end for _, _, _, end, _ in results) - timing['start']
    failed = sum(failures[slot] for selected, _, _, _, failures in results
                 for slot, (sequence, _, _, _) in enumerate(selected) if sequence >= args.warmup)
    metrics = dict(operations=args.operations, ops_per_second=args.operations / seconds,
                   latency_collected=collect_latency, failed_operations=failed,
                   successful_operations=args.operations - failed)
    if collect_latency:
        latency = [s['latency_ms'] for s in samples]
        metrics.update(mean_ms=sum(latency) / len(latency), p50_ms=percentile(latency, .5),
                       p95_ms=percentile(latency, .95), p99_ms=percentile(latency, .99), max_ms=max(latency))
    metrics.update(traffic_seconds=traffic_seconds, gc_tail_seconds=tail_seconds,
                   space_sample_every=space_every, instrumented_space_run=bool(space_every),
                   foreground_checkpoint_seconds=checkpoint_seconds,
                   traffic_gc_quiesced=settle_gc)
    result = dict(metrics=metrics, before=before, after=after, samples=samples)
    if cpu_meter:
        from cpu_accounting import CPU_SCOPE, cpu_delta
        total = cpu_delta(cpu_before, cpu_after)
        foreground = cpu_delta(cpu_before, cpu_foreground)
        tail = cpu_delta(cpu_foreground, cpu_after)
        result['cpu'] = dict(scope=CPU_SCOPE, before=cpu_before, foreground_end=cpu_foreground,
                             after=cpu_after, total=total, foreground=foreground, gc_tail=tail)
        if space_every:
            result['cpu']['scope'] += ' Instrumented space run: includes sampling work; use a separate uninstrumented CPU replay.'
        metrics.update(cpu_total_seconds=total['total_seconds'], cpu_proxy_seconds=total['proxy_seconds'],
                       cpu_storage_seconds=total['storage_seconds'], cpu_user_seconds=total['user_seconds'],
                       cpu_system_seconds=total['system_seconds'], cpu_average_cores=total['average_cores'],
                       cpu_ms_per_update=1000*total['total_seconds']/args.operations,
                       cpu_foreground_seconds=foreground['total_seconds'], cpu_gc_tail_seconds=tail['total_seconds'])
    return result

