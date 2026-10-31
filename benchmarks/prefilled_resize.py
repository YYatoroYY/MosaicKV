import argparse

from collections import Counter

import csv

from datetime import datetime, timezone

from functools import lru_cache

import hashlib

import json

import os

from pathlib import Path

import random

from statistics import mean

import subprocess

import tempfile

import time

from cluster import ROOT, STRATEGIES, DirectCluster, percentile, process_io

from small_object_resize import measure

from stats_snapshot import StatsCollectionError, require_node_stats

from cpu_accounting import CpuMeter, cpu_delta

from storage_backend import selected_backend, validate_storage

def multiply(a, b):
    result = 0
    while b:
        if b & 1:
            result ^= a
        b >>= 1
        a <<= 1
        if a & 256:
            a ^= 0x11d
    return result

def scale_table(denominator):
    coefficient = 1
    for _ in range(254):
        coefficient = multiply(coefficient, denominator)
    return bytes(multiply(coefficient, b) for b in range(256))

def verify_preload(cluster, initial, scheme):
    """Read every shard; check initial objects, zero padding and independent RS."""
    stripes, expected, occupancy = {}, {}, {}
    for key, value in initial.items():
        assert cluster.request('GET', key) == value
        meta = json.loads(cluster.request('META', key))
        assert meta['size'] == len(value) and meta['extents']
        position = 0
        for extent in meta['extents']:
            assert extent['storage'] == 'ec', 'preload left an unencoded object'
            assert extent['data_blocks'] == cluster.k and extent['parity_blocks'] == cluster.m
            assert len(extent['shards']) == cluster.k + cluster.m
            sid = extent['stripe']
            stripes[sid] = extent
            if sid not in expected:
                expected[sid] = [bytearray(extent['block_bytes']) for _ in range(cluster.k)]
                occupancy[sid] = [bytearray(extent['block_bytes']) for _ in range(cluster.k)]
            index, offset, length = extent['block_index'], extent['block_offset'], extent['length']
            assert not any(occupancy[sid][index][offset:offset + length])
            occupancy[sid][index][offset:offset + length] = b'\x01' * length
            expected[sid][index][offset:offset + length] = value[position:position + length]
            position += length
        assert position == len(value)
    occupied = []
    zero_padding = 0
    for sid, extent in stripes.items():
        shards = extent['shards']
        assert len({s['node'] for s in shards}) == cluster.k + cluster.m
        blocks = [cluster.node_request(s['node'], 'READ', s['block']) for s in shards]
        for i, shard in enumerate(shards):
            assert len(blocks[i]) == shard.get('stored_bytes', extent['block_bytes'])
            blocks[i] = blocks[i].ljust(extent['block_bytes'], b'\0')
        for i in range(cluster.k):
            assert blocks[i] == expected[sid][i], 'data or zero padding mismatch'
            used = sum(occupancy[sid][i])
            occupied.append(used)
            zero_padding += shards[i].get('stored_bytes', len(blocks[i])) - used
        for j in range(cluster.m):
            parity = bytearray(extent['block_bytes'])
            for i in range(cluster.k):
                scaled = blocks[i].translate(scale_table((cluster.k + j) ^ i))
                for offset, byte in enumerate(scaled):
                    parity[offset] ^= byte
            assert bytes(parity) == blocks[cluster.k + j], 'independent RS mismatch'
    state = json.loads(cluster.request('STATS'))
    assert state['stripes'] == len(stripes) and not state['pending_update']
    if scheme == 'mosaic':
        assert all(extent['block_bytes'] == state['alignment_chunk'] for extent in stripes.values())
        assert zero_padding == state['free_data_bytes']
        assert state['logical_live_bytes'] == sum(len(value) for value in initial.values())
    else:
        assert state['buffer_blocks'] == 0
    return dict(passed=True, objects=len(initial), stripes=len(stripes),
                shards_checked=len(stripes) * (cluster.k + cluster.m),
                data_zero_padding_bytes=zero_padding,
                data_chunk_occupied_bytes_histogram=dict(Counter(occupied)), stats=state)

def flow(result):
    a, b = result['before'], result['after']
    if result['metrics']['scheme'] == 'mosaic':
        if b['storage_backend'] == 'native':
            fields = ['full_read_bytes', 'full_write_bytes', 'range_read_bytes', 'range_write_bytes']
            def count(state, field):
                return sum(n['parity']['memory']['native'][field] for n in state['nodes'])
            return dict(mean_chunks=None, new_stripes=b['stripes'] - a['stripes'],
                        local_io_bytes={field: count(b, field) - count(a, field) for field in fields},
                        io_mode='native_ranges_no_memcached_chunk_operations')
        def writes(s):
            return sum(n['parity']['memory'].get('batch_inline_writes', 0) +
                       sum(w['writes'] for w in n['parity']['memory']['batch_io']['workers']) for n in s['nodes'])
        return dict(mean_chunks=(writes(b) - writes(a)) / (b['m'] + 1) / result['metrics']['operations'],
                    new_stripes=b['stripes'] - a['stripes'])
    fields = ['cycles', 'commits', 'conflicts', 'failures', 'source_stripes', 'output_stripes',
              'read_payload_bytes', 'write_payload_bytes', 'local_clones']
    return dict(reencoded_remainders=b['reencoded_remainders'] - a['reencoded_remainders'],
                sealed_stripes=b['regrouped_stripes'] - a['regrouped_stripes'],
                gc={f: b['gc'][f] - a['gc'][f] for f in fields},
                inplace_reencodes=b.get('inplace_reencodes', 0) - a.get('inplace_reencodes', 0),
                inplace_delta_updates=b.get('inplace_delta_updates', 0) - a.get('inplace_delta_updates', 0))

def run(args, output, repeat, k, m, scheme, initial, trace, expected, gc_paused=None, replica_cleanup=None, proxy_env=None,
        include_maintenance_tail=False, traffic_enabled=True, collect_latency=True, preloaded_compose=None):
    backend = selected_backend(args)
    skip_readback = getattr(args, 'skip_readback_validation', False)
    record_metadata = getattr(args, 'record_metadata_memory', False)
    if record_metadata:
        from metadata_accounting import snapshot as metadata_snapshot, metrics as metadata_metrics
    assert not preloaded_compose or skip_readback, 'preloaded handoff requires readback disabled'
    original_keys = len(initial)
    initial, expected = initial.copy(), expected.copy()
    padding = {}
    padding_bytes = len(next(iter(initial.values())))
    index = 0
    while len(initial) % k:
        key = f'__ec_benchmark_padding__:{index}'
        index += 1
        if key in initial:
            continue
        padding[key] = b'P' * padding_bytes
        initial[key] = expected[key] = padding[key]
    directory = output / f'r{repeat}-{k}+{m}-{scheme}'
    directory.mkdir()
    with tempfile.TemporaryDirectory(prefix='ec-prefill-resize-') as temporary:
        cluster = DirectCluster(Path(temporary) / 'compose.json', k, m, 4096,
                                strategy=STRATEGIES[scheme], coding_unit='object')
        config = json.loads(cluster.file.read_text())
        for service in config['services'].values():
            service['restart'] = 'no'
            service['environment']['STORAGE_BACKEND'] = backend
            if hasattr(args, 'native_memory_mb'):
                service['environment']['NATIVE_MEMORY_MB'] = str(args.native_memory_mb)
            service['environment']['EC_PROFILE'] = '0'
            service['environment']['EC_TRAFFIC'] = str(int(traffic_enabled))
            service['environment']['EC_WORKLOAD_PHASE'] = 'load'
            if 'MEMCACHED_MEMORY_MB' in service['environment'] and hasattr(args, 'memcached_memory_mb'):
                service['environment']['MEMCACHED_MEMORY_MB'] = str(args.memcached_memory_mb)
        if gc_paused is not None:
            assert scheme == 'fastgc'
            config['services']['proxy']['environment']['GC_PAUSED'] = str(int(gc_paused))
        if replica_cleanup is not None:
            assert scheme in ('foreground_regroup', 'fastgc', 'inplace')
            config['services']['proxy']['environment']['TEMP_REPLICA_CLEANUP'] = str(int(replica_cleanup))
        if proxy_env:
            config['services']['proxy']['environment'].update(proxy_env)
        if preloaded_compose:
            # Take over a fully loaded, idle cluster without recreating containers.
            # Validate the target before entering the teardown-owning try/finally.
            existing = json.loads(Path(preloaded_compose).read_text())
            assert existing['services'].keys() == config['services'].keys()
            for name, service in config['services'].items():
                assert existing['services'][name]['environment'] == service['environment'], name
                assert existing['services'][name]['command'] == service['command'], name
            cluster.file = Path(preloaded_compose).resolve()
            cluster.port = int(existing['services']['proxy']['ports'][0].split(':')[-2])
            ready = cluster.ready()
            assert ready['workload_phase'] == 'load' and ready['updates'] == 0
            assert ready['keys'] == ready['puts'] == len(initial) and not ready['pending_update']
            assert ready['k'] == k and ready['m'] == m and ready['update_strategy'] == STRATEGIES[scheme]
            config = existing
        else:
            cluster.file.write_text(json.dumps(config))
        (directory / 'compose.json').write_text(json.dumps(config, indent=2))
        try:
            if not preloaded_compose:
                cluster.compose('up', '-d', '--no-build', '--wait')
            ready = cluster.ready()
            validate_storage(ready, scheme, backend)
            cluster.connect_nodes()
            cpu_meter = CpuMeter(cluster)
            load_cpu_before = cpu_meter.snapshot()
            if preloaded_compose:
                image_id = subprocess.check_output(['docker', 'image', 'inspect', os.environ['EC_IMAGE'],
                                                   '--format', '{{.Id}}'], text=True).strip()
                assert all(c['Image'] == image_id for c in cluster.containers)
            with cluster.persistent_requests() as load_requests:
                if not preloaded_compose:
                    started = time.monotonic()
                    for number, (key, value) in enumerate(initial.items(), 1):
                        cluster.request('PUT', key, value)
                        if number % 10000 == 0 or number == len(initial):
                            elapsed = time.monotonic() - started
                            print(f'PRELOAD {scheme} {number}/{len(initial)} elapsed={elapsed:.1f}s '
                                  f'average={number / elapsed:.1f} keys/s connections={load_requests.connections}', flush=True)
                load_cpu_after = cpu_meter.snapshot()
                if skip_readback:
                    state = json.loads(cluster.request('STATS'))
                    assert state['keys'] == state['puts'] == len(initial)
                    assert state['updates'] == 0 and not state['pending_update']
                    assert state['stripes'] > 0
                    if scheme == 'mosaic':
                        assert state['logical_live_bytes'] == sum(map(len, initial.values()))
                    else:
                        assert state['buffer_blocks'] == 0
                    preload = dict(passed=True, validation_scope='counters_only', readback_performed=False,
                                   values_checked=0, shards_checked=0, objects=len(initial),
                                   stripes=state['stripes'], stats=state)
                    print(f'PRELOAD {scheme}: readback skipped; reused_loaded_cluster={bool(preloaded_compose)}', flush=True)
                else:
                    preload = verify_preload(cluster, initial, scheme)
                    preload.update(validation_scope='full_readback', readback_performed=True)
            preload['reused_loaded_cluster'] = bool(preloaded_compose)
            preload['cpu'] = (None if preloaded_compose else dict(
                before=load_cpu_before, after=load_cpu_after,
                summary=cpu_delta(load_cpu_before, load_cpu_after)))
            preload['client'] = dict(mode='persistent_sequential_req',
                                     connections=load_requests.connections, requests=load_requests.requests)
            load_state = preload['stats']
            require_node_stats(load_state, 'preload')
            assert load_state['workload_phase'] == 'load'
            sender_groups = [load_state['node_requests'], *(n['parity'] for n in load_state['nodes'])]
            if 'buffer_replication' in load_state:
                sender_groups.append(load_state['buffer_replication'])
            for group in sender_groups:
                assert not group['traffic_enabled']
                assert all(link['total_bytes'] == link['requests'] == link['replies'] == 0 for link in group['rpc_links'])
            assert all(n['parity']['traffic']['received_bytes'] == n['parity']['traffic']['sent_bytes'] == 0
                       for n in load_state['nodes'])
            if scheme == 'mosaic':
                assert load_state['initial_placement'] == 'round_robin'
                assert load_state['load_elapsed_ms'] == load_state['committed_object_updates'] == 0
                assert all(n['cumulative_bytes'] == 0 for role in ['data_load', 'parity_load'] for n in load_state[role])
            preload.update(original_objects=original_keys, padding_objects=len(padding),
                           padding_lengths={key: len(value) for key, value in padding.items()})
            if record_metadata:
                preload['metadata_memory'] = metadata_snapshot(cluster, len(initial), 'preload')
                (directory / 'metadata-memory.json').write_text(json.dumps(
                    dict(complete=False, preload=preload['metadata_memory']), indent=2) + '\n')
            (directory / 'preload.json').write_text(json.dumps(preload, indent=2))
            print(f'r{repeat} {k}+{m} {scheme}: preload {len(initial)} objects, '
                  f'{preload["stripes"]} EC groups; {preload["validation_scope"]}', flush=True)
            if scheme == 'fastgc':
                cluster.request('GC', 'RESET_SPACE_PEAKS')
            cluster.request('PHASE', 'RESIZE')
            print(f'UPDATE_START {scheme}: {len(trace)} updates', flush=True)
            result = measure(cluster, args, 1, trace, settle_gc=include_maintenance_tail and scheme == 'fastgc',
                             collect_latency=collect_latency, checkpoint_directory=directory, cpu_meter=cpu_meter)
            result['storage_backend'] = backend
            if record_metadata:
                if scheme == 'fastgc':
                    assert result['metrics']['traffic_gc_quiesced'], 'final metadata requires completed GC tail'
                final_metadata = metadata_snapshot(cluster, len(initial), 'final')
                result['metadata_memory'] = dict(complete=True, preload=preload['metadata_memory'], final=final_metadata)
                result['metrics'].update(metadata_metrics(preload['metadata_memory'], final_metadata))
                (directory / 'metadata-memory.json').write_text(json.dumps(result['metadata_memory'], indent=2) + '\n')
            result['cpu']['preload'] = preload['cpu']
            result['metrics']['cpu_preload_seconds'] = (preload['cpu']['summary']['total_seconds']
                                                       if preload['cpu'] is not None else None)
            (directory / 'cpu-accounting.json').write_text(json.dumps(result['cpu'], indent=2))
            assert result['metrics']['failed_operations'] == 0
            result['metrics'] = dict(k=k, m=m, scheme=scheme, repeat=repeat, clients=1,
                                     original_keys=original_keys, preload_keys=len(initial),
                                     padding_objects=len(padding), padding_value_bytes=sum(map(len, padding.values())),
                                     traffic_enabled=traffic_enabled, **result['metrics'])
            if scheme == 'mosaic':
                result['metrics']['alignment_chunk'] = result['after']['alignment_chunk']
            if scheme == 'fastgc':
                result['metrics']['gc_every_updates'] = result['after']['gc'].get('every_updates', 0)
            assert result['after']['node_requests']['traffic_enabled'] == traffic_enabled
            assert all(n['parity']['traffic']['enabled'] == traffic_enabled for n in result['after']['nodes'])
            if gc_paused is not None:
                result['metrics']['gc_paused'] = gc_paused
                assert result['before']['gc']['paused'] == gc_paused
            if replica_cleanup is not None:
                result['metrics']['replica_cleanup'] = replica_cleanup
                assert result['before']['temporary_replica_cleanup'] == replica_cleanup
                if scheme == 'inplace':
                    assert result['after']['buffer_delete_requests'] == result['before']['buffer_delete_requests']
                    assert result['after']['buffer_blocks'] == result['after']['retired_buffers'] == result['after']['gc']['retired_stripes'] == 0
                    for a, b in zip(result['before']['nodes'], result['after']['nodes']):
                        for name in ('buffers', 'deleted-buffers'):
                            assert a['parity']['memory']['groups'].get(name) == b['parity']['memory']['groups'].get(name)
                elif not replica_cleanup:
                    assert result['after']['buffer_delete_requests'] == 0
                    assert result['after']['retired_buffers'] == 0
                    for state, count in [(result['before'], len(initial)), (result['after'], len(initial) + len(trace))]:
                        groups = [n['parity']['memory']['groups'] for n in state['nodes']]
                        assert sum(g.get('buffers', {}).get('entries', 0) for g in groups) == count * m
                        assert sum(g.get('deleted-buffers', {}).get('entries', 0) for g in groups) == 0
                else:
                    assert result['after']['buffer_delete_requests'] > result['before']['buffer_delete_requests']
                    for state, count in [(result['before'], len(initial)), (result['after'], len(initial) + len(trace))]:
                        groups = [n['parity']['memory']['groups'] for n in state['nodes']]
                        live_copies = sum(g.get('buffers', {}).get('entries', 0) for g in groups)
                        fences = sum(g.get('deleted-buffers', {}).get('entries', 0) for g in groups)
                        assert fences == state.get('buffer_delete_items', state['buffer_delete_requests'])
                        assert live_copies + fences == count * m
                        assert live_copies == (state['buffer_blocks'] + state['retired_buffers']) * m
            result['flow'] = flow(result)
            (directory / 'measurement.json').write_text(json.dumps(result))
            if scheme == 'fastgc':
                cluster.request('GC', 'PAUSE')
                deadline = time.monotonic() + 60
                while json.loads(cluster.request('GC', 'STATUS'))['phase'] != 'idle':
                    assert time.monotonic() < deadline
                    time.sleep(.05)
            metadata, selected = {}, []
            validation_client = dict(mode='disabled', connections=0, requests=0)
            if not skip_readback:
                with cluster.persistent_requests() as validation_requests:
                    for key, value in expected.items():
                        assert cluster.request('GET', key) == value
                        metadata[key] = json.loads(cluster.request('META', key))
                    selected = sorted(metadata, key=lambda key: len(metadata[key]['extents']), reverse=True)[:k]
                    cluster.verify_parity({key: expected[key] for key in selected})
                validation_client = dict(mode='persistent_sequential_req', connections=validation_requests.connections,
                                         requests=validation_requests.requests)
            io = {s: process_io(cluster, s) for s in ['proxy', *(f'node{i}' for i in range(k + m))]}
            validate_storage(result['after'], scheme, backend)
            validation = dict(passed=True, values_checked=len(metadata), parity_sample_objects=len(selected),
                              validation_scope='counters_and_io_only' if skip_readback else 'full_readback',
                              readback_performed=not skip_readback,
                              fragments=dict(Counter(len(v['extents']) for v in metadata.values())),
                              mean_fragments=(sum(len(v['extents']) for key, v in metadata.items() if key not in padding) / original_keys if metadata else None), process_io=io,
                              client=validation_client)
            (directory / 'validation.json').write_text(json.dumps(validation, indent=2))
            row = result['metrics']
            if collect_latency:
                print(f'r{repeat} {k}+{m} {scheme}: mean={row["mean_ms"]:.3f}ms '
                      f'p50={row["p50_ms"]:.3f}ms p99={row["p99_ms"]:.3f}ms {validation["validation_scope"]}', flush=True)
            else:
                print(f'r{repeat} {k}+{m} {scheme}: traffic replay {validation["validation_scope"]} (latency sampling disabled)', flush=True)
        except BaseException as error:
            failure = {'passed': False, 'error': repr(error)}
            if isinstance(error, StatsCollectionError):
                # Keep the failing snapshot; a second query may hide the error
                # or wait behind the original timed-out diagnostic request.
                failure['stats'] = error.snapshot
            else:
                try:
                    failure['stats'] = json.loads(cluster.request('STATS'))
                except Exception as diagnostic_error:
                    failure['stats_error'] = repr(diagnostic_error)
            (directory / 'failure.json').write_text(json.dumps(failure, indent=2))
            raise
        finally:
            for sock in getattr(cluster, 'node_sockets', {}).values():
                sock.close()
            try:
                (directory / 'containers.log').write_text(cluster.compose('logs', '--no-color'))
            finally:
                cluster.compose('down', '--volumes', '--timeout', '0')
                cluster.context.term()

