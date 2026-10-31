#!/usr/bin/env python3
"""Verify selective data reads, shortened stripes, and degraded-read fallback."""
import json
from pathlib import Path
import tempfile

from integration import Cluster
from process_io import process_io


def check_read(cluster, key, value, expected_reads, status=b'OK'):
    before = json.loads(cluster.request('STATS'))
    reply = cluster.request('GET', key, status=status)
    if status == b'OK':
        assert reply == value
    after = json.loads(cluster.request('STATS'))
    tasks = sum(w['tasks'] for w in after['node_requests']['workers']) - sum(
        w['tasks'] for w in before['node_requests']['workers']) - cluster.k - cluster.m
    assert tasks == expected_reads, (key, tasks, expected_reads)


def run(strategy, unit):
    with tempfile.TemporaryDirectory(prefix='ec-single-kv-read-') as tmp:
        cluster = Cluster(Path(tmp) / 'compose.json', 3, 2, 128, strategy=strategy, coding_unit=unit)
        config = json.loads(cluster.file.read_text())
        config['services']['proxy']['environment'].update(GC_PAUSED='1', TEMP_REPLICA_CLEANUP='0')
        cluster.file.write_text(json.dumps(config))
        try:
            cluster.compose('up', '-d', '--no-build', '--wait')
            cluster.ready()
            sizes = [0, 1, 255, 512, 1025, 193]
            values = {f'k{i}': bytes((j + i) % 256 for j in range(size)) for i, size in enumerate(sizes)}
            for key, value in values.items():
                cluster.request('PUT', key, value)
            def check_all():
                for key, value in values.items():
                    meta = json.loads(cluster.request('META', key))
                    count = sum((e['block_offset'] + e['length'] + e['block_bytes'] - 1) // e['block_bytes']
                                for e in meta['extents'] if e['storage'] == 'ec' and e['length'])
                    check_read(cluster, key, value, count)
                cluster.verify_parity(values)
            check_all()
            if unit == 'object':
                cluster.request('UPDATE', 'k0', b'R' * 128)
                values['k0'] = b'R' * 128
                check_all()  # Includes buffer reads and relocated/shortened survivor stripes.
                meta = json.loads(cluster.request('META', 'k4'))['extents'][0]
                target = meta['shards'][meta['block_index']]
                original = cluster.node_request(target['node'], 'READ', target['block'])
                cluster.node_request(target['node'], 'WRITE', target['block'], b'X' * 64)
                check_read(cluster, 'k4', values['k4'], 1 + meta['data_blocks'])  # Direct miss, then only K helpers.
                cluster.node_request(target['node'], 'WRITE', target['block'], original)
                # Losing unrelated shards must not prevent a healthy target read,
                # even when the whole stripe can no longer be reconstructed.
                for shard in meta['shards']:
                    if shard != target:
                        cluster.node_request(shard['node'], 'DELETE', shard['block'])
                check_read(cluster, 'k4', values['k4'], 1)
                cluster.node_request(target['node'], 'DELETE', target['block'])
                check_read(cluster, 'k4', None, len(meta['shards']), status=b'UNAVAILABLE')  # Each helper tried once.
            for service in ['proxy', *(f'node{i}' for i in range(5))]:
                process_io(cluster, service)
            print(f'PASS {strategy}/{unit}: selective reads, binary/empty/range values, EC parity and failure handling', flush=True)
        finally:
            cluster.compose('down', '--volumes', '--timeout', '0')
            cluster.context.term()


if __name__ == '__main__':
    run('foreground_regroup', 'object')
    run('fastgc', 'object')
    run('rmw', 'block')
