#!/usr/bin/env python3
"""Build and characterize a content-preserving ShareGPT prefix KV workload.

Initialization is separate from timed updates. The compact bundle supports both
APPEND and full UPDATE replay without storing every repeated context prefix.
This is a derived storage workload, not an observed database or arrival trace.
"""
import argparse
import base64
from collections import Counter
import csv
import hashlib
import json
import mmap
from pathlib import Path
import random
import struct

import numpy as np

def json_array(path):
    """Stream a top-level JSON array without retaining the complete source."""
    decoder = json.JSONDecoder()
    with path.open() as stream:
        buffer = ''
        eof = False
        started = False
        while True:
            buffer = buffer.lstrip()
            if not buffer and not eof:
                buffer += stream.read(1024 * 1024)
                if not buffer:
                    eof = True
                continue
            if not started:
                assert buffer.startswith('['), path
                buffer = buffer[1:]
                started = True
                continue
            if buffer.startswith(','):
                buffer = buffer[1:]
                continue
            if buffer.startswith(']'):
                return
            try:
                value, offset = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    raise
                buffer += chunk
                continue
            yield value
            buffer = buffer[offset:]

def encoded(messages):
    return [(json.dumps(m, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()
            for m in messages]

ROOT = Path(__file__).resolve().parent
SOURCE = None
DEFAULT_OUTPUT = Path('workloads/sharegpt')
ORDER = struct.Struct('<II')  # session index, zero-based message ordinal (>=1)


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def file_hash(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def stats(values):
    a = np.asarray(values, dtype=np.float64)
    return dict(count=len(a), mean=float(a.mean()), total=float(a.sum()),
                cv=float(a.std() / a.mean()),
                **dict(zip(['min', 'p10', 'p50', 'p90', 'p95', 'p99', 'max'],
                           map(float, np.percentile(a, [0, 10, 50, 90, 95, 99, 100])))))


def load_sessions(bundle):
    with (Path(bundle) / 'sessions.jsonl').open() as f:
        return [json.loads(line) for line in f]


def iter_initial(bundle):
    """Yield (key, initial_value_bytes) for LOAD, in saved stripe load order."""
    with (Path(bundle) / 'dataset/initial_kv.jsonl').open() as f:
        for line in f:
            row = json.loads(line)
            value = base64.b64decode(row['value_b64'], validate=True)
            assert len(value) == row['value_bytes']
            assert hashlib.sha256(value).hexdigest() == row['value_sha256']
            yield row['key'], value


def iter_updates(bundle, full_update=False):
    """Yield (key, payload, operation, old_bytes, new_bytes) for RESIZE.

    Both modes use precisely the same schedule and logical values. Full UPDATE
    payloads include the whole prefix; APPEND payloads include only the next line.
    """
    bundle = Path(bundle)
    sessions = load_sessions(bundle)
    cursors = [1] * len(sessions)
    lengths = [s['message_lengths'][0] for s in sessions]
    with (bundle / 'messages.bin').open('rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
            for sid, ordinal in ORDER.iter_unpack((bundle / 'testset/schedule.bin').read_bytes()):
                s = sessions[sid]
                assert ordinal == cursors[sid]
                old = lengths[sid]
                new = old + s['message_lengths'][ordinal]
                start = s['offset'] + (0 if full_update else old)
                yield s['key'], data[start:s['offset'] + new], 'UPDATE' if full_update else 'APPEND', old, new
                lengths[sid], cursors[sid] = new, ordinal + 1
    assert all(c == len(s['message_lengths']) for c, s in zip(cursors, sessions))


def build(output, seed):
    source = json.loads((ROOT / 'data/sharegpt.source.json').read_text())
    for entry in source['files']:
        path = SOURCE / entry['path']
        assert path.stat().st_size == entry['bytes']
        assert file_hash(path) == entry['sha256']
    output.mkdir(parents=True, exist_ok=False)
    (output / 'dataset').mkdir()
    (output / 'testset').mkdir()
    seen, ids, sessions, role_counts = set(), set(), [], Counter()
    occurrences = duplicates = offset = 0
    with (output / 'messages.bin').open('wb') as data, (output / 'sessions.jsonl').open('w') as index:
        for entry in source['files']:
            for row in json_array(SOURCE / entry['path']):
                occurrences += 1
                messages = [dict(role={'human': 'user', 'gpt': 'assistant'}.get(m['from'], m['from']),
                                 content=m['value']) for m in row['conversations']]
                assert messages
                payloads = encoded(messages)
                value = b''.join(payloads)
                digest = hashlib.sha256(value).hexdigest()
                if digest in seen:
                    duplicates += 1
                    continue
                seen.add(digest)
                key = 'sharegpt:' + row['id']
                assert key not in ids
                ids.add(key)
                record = dict(key=key, source_id=row['id'], source_file=entry['path'],
                              offset=offset, message_lengths=[len(p) for p in payloads],
                              final_bytes=len(value), final_sha256=digest)
                sessions.append(record)
                index.write(json.dumps(record, ensure_ascii=False) + '\n')
                data.write(value)
                offset += len(value)
                role_counts.update(m['role'] for m in messages)
    print(json.dumps(dict(stage='encoded', keys=len(sessions), source_rows=occurrences,
                          duplicates=duplicates, value_bytes=offset)), flush=True)

    load_order = list(range(len(sessions)))
    random.Random(seed).shuffle(load_order)
    with (output / 'messages.bin').open('rb') as f, (output / 'dataset/initial_kv.jsonl').open('w') as out:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
            for sid in load_order:
                s = sessions[sid]
                value = data[s['offset']:s['offset'] + s['message_lengths'][0]]
                out.write(json.dumps(dict(key=s['key'], value_b64=base64.b64encode(value).decode(),
                    value_bytes=len(value), value_sha256=hashlib.sha256(value).hexdigest())) + '\n')
    counts = np.array([len(s['message_lengths']) - 1 for s in sessions], dtype=np.int64)
    labels = np.repeat(np.arange(len(sessions), dtype=np.uint32), counts)
    np.random.default_rng(seed).shuffle(labels)
    cursors = np.ones(len(sessions), dtype=np.int64)
    with (output / 'testset/schedule.bin').open('wb') as out:
        for sid in labels:
            out.write(ORDER.pack(int(sid), int(cursors[sid])))
            cursors[sid] += 1
    assert np.array_equal(cursors - 1, counts)
    write_json(output / 'expected_final.json', {s['key']: dict(bytes=s['final_bytes'], sha256=s['final_sha256']) for s in sessions})
    write_json(output / 'source.json', source)
    manifest = dict(format='sharegpt-prefix-kv-v1', seed=seed, keys=len(sessions),
        updates=int(counts.sum()), source_rows=occurrences, exact_content_duplicates_removed=duplicates,
        role_counts=dict(role_counts), value_encoding='UTF-8 canonical JSONL, role/content per line, trailing newline',
        key_policy='sharegpt:<source conversation id>, unique after exact full-content deduplication',
        initialization='First message per key; all keys loaded before any measured update; saved random load order',
        updates_policy='Each later message appends one JSONL record; full UPDATE mode reconstructs the same logical prefix',
        schedule_policy='Seeded shuffle of update labels, consuming each key in chronological order; no initial PUTs in testset',
        schedule_format=ORDER.format, source_timestamps=False, recorded_compaction=False,
        selection='Both complete HTML-cleaned files; no size filtering, truncation, token splitting or synthetic summary',
        runtime_padding='Not in source data. For K=6 add two separate dummy keys to complete the last stripe; never update them',
        replay_api='prepare_sharegpt.iter_initial(bundle), prepare_sharegpt.iter_updates(bundle, full_update=False/True)',
        files={name: dict(bytes=(output / name).stat().st_size, sha256=file_hash(output / name)) for name in
               ['messages.bin', 'sessions.jsonl', 'dataset/initial_kv.jsonl', 'testset/schedule.bin', 'expected_final.json', 'source.json']})
    write_json(output / 'manifest.json', manifest)
    return sessions, load_order


def analyze(output, sessions, load_order):
    initial, final, targets, increments, relative, growth, counts = [], [], [], [], [], [], []
    prefixes = []
    for s in sessions:
        sizes = np.asarray(s['message_lengths'], dtype=np.int64)
        prefix = sizes.cumsum()
        prefixes.append(prefix)
        initial.append(int(prefix[0]))
        final.append(int(prefix[-1]))
        targets.extend(prefix[1:])
        increments.extend(sizes[1:])
        relative.extend(sizes[1:] / prefix[:-1])
        growth.append(prefix[-1] / prefix[0])
        counts.append(len(sizes) - 1)
    initial, final, targets, increments, counts = map(np.asarray, [initial, final, targets, increments, counts])
    report = dict(keys=len(sessions), updates=len(targets), units='bytes unless explicitly named ratio/count',
                  distributions=dict(initial_value_bytes=stats(initial), final_value_bytes=stats(final),
                  update_target_value_bytes=stats(targets), append_payload_bytes=stats(increments),
                  updates_per_key=stats(counts), final_over_initial_ratio=stats(growth),
                  append_over_old_value_ratio=stats(relative)),
                  growth=dict(strict_append_updates=len(targets), prefix_rewrites=0, shrinking_updates=0,
                              equal_size_updates=0, logical_live_bytes_growth_ratio=float(final.sum()/initial.sum())),
                  traffic=dict(initial_bytes=int(initial.sum()), final_live_bytes=int(final.sum()),
                               append_update_bytes=int(increments.sum()), full_update_bytes=int(targets.sum()),
                               full_update_over_append_payload_ratio=float(targets.sum()/increments.sum()),
                               note='Logical API payload only; not backend EC/network write amplification'),
                  final_p90_over_p10=float(np.percentile(final,90)/np.percentile(final,10)),
                  final_size_cdf={str(k)+'_KiB':float(np.mean(final <= k*1024)) for k in [1,4,8,16,32,64,128,256,512]},
                  append_size_cdf={str(k)+'_KiB':float(np.mean(increments <= k*1024)) for k in [1,4,8,16,32]},
                  update_target_size_cdf={str(k)+'_KiB':float(np.mean(targets <= k*1024)) for k in [4,16,32,64,128,256]},
                  largest_10pct_keys=dict(count=int(np.ceil(len(final)*.1))),
                  observed_read_delete_or_summary_operations=False)
    largest = np.argsort(final)[-int(np.ceil(len(final)*.1)):]
    report['largest_10pct_keys'].update(final_live_byte_fraction=float(final[largest].sum()/final.sum()),
                                      update_count_fraction=float(counts[largest].sum()/counts.sum()))
    # Static 6+3 InPlace capacity model with actual data bytes and max-sized parity.
    # This does not model allocation buckets, fragmentation, GC or metadata.
    current = initial.copy()
    group_order = np.asarray(load_order)
    def checkpoint(step):
        ordered = current[group_order]
        padded = np.pad(ordered, (0, (-len(ordered)) % 6))
        live = int(current.sum())
        parity = 3 * int(padded.reshape(-1, 6).max(axis=1).sum())
        d = stats(current)
        return dict(updates=step, progress=step/len(targets), live_bytes=live,
                    mean_bytes=d['mean'], p10_bytes=d['p10'], p50_bytes=d['p50'],
                    p90_bytes=d['p90'], p99_bytes=d['p99'], cv=d['cv'],
                    estimated_inplace_sa=(live+parity)/(1.5*live))
    checkpoints = [checkpoint(0)]
    marks = set(round(len(targets)*i/10) for i in range(1,11))
    cursors = np.ones(len(sessions), dtype=np.int64)
    switches, previous = 0, None
    for step, (sid, ordinal) in enumerate(ORDER.iter_unpack((output/'testset/schedule.bin').read_bytes()),1):
        assert cursors[sid] == ordinal
        assert current[sid] == prefixes[sid][ordinal-1]
        current[sid] = prefixes[sid][ordinal]
        cursors[sid] += 1
        if previous is not None and previous != sid:
            switches += 1
        previous = sid
        if step in marks:
            checkpoints.append(checkpoint(step))
    assert np.array_equal(current,final)
    assert np.array_equal(cursors-1,counts)
    report['schedule'] = dict(updates=len(targets), adjacent_same_key_fraction=1-switches/(len(targets)-1),
                              all_initialization_before_updates=True, per_key_chronology_verified=True)
    report['capacity_model'] = dict(k=6,m=3,formula='(sum(value lengths) + 3*sum(stripe max length))/(1.5*sum(value lengths))',
        grouping='Fixed groups from saved randomized initialization order; final partial group padded with zero-length dummies',
        limitations='Capacity estimate only; excludes allocator rounding, metadata, stale versions, GC and real dummy bytes; not measured performance',
        checkpoints=checkpoints)
    write_json(output/'analysis.json',report)
    with (output/'growth.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(checkpoints[0]))
        writer.writeheader();writer.writerows(checkpoints)
    return report


def verify(output):
    sessions = load_sessions(output)
    state = {key:bytearray(value) for key,value in iter_initial(output)}
    count = 0
    # One full sequential replay checks every saved update and final content hash.
    for key,payload,operation,old,new in iter_updates(output):
        assert operation == 'APPEND' and len(state[key]) == old
        state[key].extend(payload)
        assert len(state[key]) == new and len(payload) == new-old and new > old
        count += 1
    for s in sessions:
        value=state[s['key']]
        assert len(value) == s['final_bytes']
        assert hashlib.sha256(value).hexdigest() == s['final_sha256']
    state.clear()
    # Verify the baseline full-value interface against the same prefix history.
    state = dict(iter_initial(output))
    full_count = 0
    for key,payload,operation,old,new in iter_updates(output, full_update=True):
        assert operation == 'UPDATE' and len(state[key]) == old
        assert len(payload) == new and payload.startswith(state[key])
        state[key] = payload
        full_count += 1
    assert full_count == count
    for s in sessions:
        assert hashlib.sha256(state[s['key']]).hexdigest() == s['final_sha256']
    manifest=json.loads((output/'manifest.json').read_text())
    for name,entry in manifest['files'].items():
        assert (output/name).stat().st_size == entry['bytes']
        assert file_hash(output/name) == entry['sha256']
    assert count == manifest['updates']
    result=dict(passed=True,keys=len(state),updates=count,all_initial_hashes_verified=True,
                all_update_lengths_and_per_key_order_verified=True,all_final_content_hashes_verified=True,
                all_bundle_file_hashes_verified=True,source_hashes_verified_at_build=True,
                
                full_update_replay_prefixes_and_final_hashes_verified=True,
                full_update_operations=full_count)
    write_json(output/'validation.json',result)
    return result


def main():
    global SOURCE
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, default=Path('workloads/sharegpt-source'))
    p.add_argument('--download', action='store_true', help='download pinned source files and verify SHA-256')
    p.add_argument('--output',type=Path,default=DEFAULT_OUTPUT)
    p.add_argument('--seed',type=int,default=20261018)
    args=p.parse_args()
    SOURCE = args.source.resolve()
    if args.download:
        import urllib.request
        SOURCE.mkdir(parents=True, exist_ok=True)
        source = json.loads((ROOT / 'data/sharegpt.source.json').read_text())
        for entry in source['files']:
            target = SOURCE / entry['path']
            if target.exists() and file_hash(target) == entry['sha256']:
                continue
            temporary = target.with_suffix('.download')
            with urllib.request.urlopen(entry['url'], timeout=120) as response, temporary.open('wb') as out:
                import shutil
                shutil.copyfileobj(response, out)
            if file_hash(temporary) != entry['sha256']:
                raise ValueError('ShareGPT source checksum mismatch')
            temporary.replace(target)
    sessions,order=build(args.output,args.seed)
    report=analyze(args.output,sessions,order)
    print(json.dumps(dict(stage='analyzed',statistics=report)),flush=True)
    print(json.dumps(dict(stage='verified',output=str(args.output),validation=verify(args.output))),flush=True)


if __name__=='__main__':
    main()
