#!/usr/bin/env python3
"""Docker integration checks for physical-endpoint benchmark clients."""
import base64
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

from integration import Cluster

ROOT = Path(__file__).resolve().parents[1]


def sharegpt_fixture(directory):
    (directory / 'dataset').mkdir(parents=True)
    (directory / 'testset').mkdir()
    sessions, initial, final, messages, schedule = [], [], {}, bytearray(), bytearray()
    for sid in range(6):
        key = f'sharegpt:fixture-{sid}'
        parts = [(json.dumps(dict(role='user', content=f'{sid}:{i}:' + 'x' * 70)) + '\n').encode()
                 for i in range(3)]
        payload = b''.join(parts)
        digest = hashlib.sha256(payload).hexdigest()
        sessions.append(dict(key=key, offset=len(messages), message_lengths=list(map(len, parts)),
                             final_bytes=len(payload), final_sha256=digest))
        initial.append(dict(key=key, value_b64=base64.b64encode(parts[0]).decode(),
                            value_bytes=len(parts[0]), value_sha256=hashlib.sha256(parts[0]).hexdigest()))
        messages.extend(payload)
        final[key] = dict(bytes=len(payload), sha256=digest)
    for ordinal in (1, 2):
        for sid in range(6):
            schedule.extend(struct.pack('<II', sid, ordinal))
    (directory / 'messages.bin').write_bytes(messages)
    (directory / 'testset/schedule.bin').write_bytes(schedule)
    for name, rows in [('sessions.jsonl', sessions), ('dataset/initial_kv.jsonl', initial)]:
        (directory / name).write_text(''.join(json.dumps(row) + '\n' for row in rows))
    (directory / 'expected_final.json').write_text(json.dumps(final))
    files = {str(path.relative_to(directory)): dict(sha256=hashlib.sha256(path.read_bytes()).hexdigest())
             for path in directory.rglob('*') if path.is_file()}
    (directory / 'manifest.json').write_text(json.dumps(dict(keys=6, updates=12, files=files)))


def main():
    os.environ.setdefault('EC_IMAGE', 'mosaickv:local')
    with tempfile.TemporaryDirectory(prefix='mosaickv-endpoint-smoke-') as temporary:
        root = Path(temporary)
        initial = {f'key{i}': 64 for i in range(12)}
        for name, fixed in [('resize', False), ('fixed', True)]:
            current, updates = initial.copy(), []
            for i in range(24):
                key = f'key{i % 12}'
                length = 64 if fixed else 80 + (i // 12) * 16
                updates.append(dict(key=key, old_bytes=current[key], new_bytes=length))
                current[key] = length
            (root / f'{name}.json').write_text(json.dumps(dict(initial_lengths=initial, updates=updates)))
        sharegpt_fixture(root / 'sharegpt')
        for scheme in ['mosaic', 'foreground_regroup', 'fastgc', 'inplace']:
            cases = [('resize', 'update', 'full'), ('resize', 'read', 'full'),
                     ('resize', 'repair', 'full'), ('fixed', 'update', 'full'),
                     ('sharegpt', 'update', 'full')]
            if scheme == 'mosaic':
                cases.append(('sharegpt', 'update', 'append'))
            for source, experiment, mode in cases:
                label = f'{scheme}-{source}-{experiment}-{mode}'
                cluster = Cluster(root / 'compose.json', 2, 1, 4096, strategy=scheme, coding_unit='object')
                config = json.loads(cluster.file.read_text())
                for service in config['services'].values():
                    service['restart'] = 'no'
                    service['environment'].update(STORAGE_BACKEND='native', NATIVE_MEMORY_MB='64',
                                                  EC_WORKLOAD_PHASE='load', EC_TRAFFIC='1')
                config['services']['proxy']['environment'].update(ALIGNMENT_CHUNK='128', MOSAIC_APPEND='1',
                                                                  GC_EVERY_UPDATES='5')
                cluster.file.write_text(json.dumps(config))
                try:
                    cluster.compose('up', '-d', '--no-build', '--wait')
                    cluster.ready()
                    source_args = (['--sharegpt-bundle', str(root / 'sharegpt')] if source == 'sharegpt'
                                   else ['--workload-file', str(root / f'{source}.json')])
                    out = root / label
                    measurement_args = []
                    if experiment == 'update':
                        measurement_args = ['--space-sample-every', '5']
                    else:
                        # Exercise the host collector against real service cgroups.
                        inventory = root / f'{label}-cpu-hosts.json'
                        ids = cluster.compose('ps', '-q').split()
                        containers = json.loads(subprocess.check_output(['docker', 'inspect', *ids], text=True))
                        services = {c['Config']['Labels']['com.docker.compose.service']:
                                    dict(host='local', container=c['Name'].lstrip('/'))
                                    for c in containers}
                        inventory.write_text(json.dumps(dict(services=services)))
                        measurement_args = ['--cpu-hosts', str(inventory)]
                    subprocess.run([sys.executable, str(ROOT / 'experiments/run_endpoint.py'),
                                    '--endpoint', f'tcp://127.0.0.1:{cluster.port}', *source_args,
                                    '--experiment', experiment, '--update-mode', mode,
                                    '--reads', '20', '--repair-count', '5', '--record-metadata-memory',
                                    *measurement_args, '--output', str(out)], check=True, stdout=subprocess.DEVNULL, timeout=180)
                    result = json.loads((out / 'metrics.json').read_text())
                    assert json.loads((out / 'manifest.json').read_text())['passed']
                    assert result['mean_ms'] > 0 and result['mean_fragments'] >= 1
                    assert result['storage_rx_tx_plus_proxy_backend_tx_bytes'] > 0
                    assert result['space_amplification'] > 0
                    if experiment == 'update':
                        assert (out / 'space-trace.csv').is_file()
                        assert result['instrumented_space_run']
                    else:
                        assert result['cpu_ms_per_operation'] >= 0
                        assert len(json.loads((out / 'cpu-accounting.json').read_text())['total']['services']) == 4
                    assert result['operations'] == (20 if experiment == 'read' else 5 if experiment == 'repair'
                                                     else 12 if source == 'sharegpt' else 24)
                    print('PASS', label, flush=True)
                finally:
                    cluster.compose('down', '--remove-orphans')
                    cluster.context.term()


if __name__ == '__main__':
    main()
