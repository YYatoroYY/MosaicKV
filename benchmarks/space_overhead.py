#!/usr/bin/env python3
"""Report data padding, allocator holes and invalid data from benchmark snapshots."""
import argparse
import csv
import json
from pathlib import Path
from statistics import mean
from space_accounting import mosaic_space


def snapshot(state, scheme):
    assert not state['pending_update'], 'space accounting requires a committed snapshot'
    if scheme == 'mosaic':
        logical = state['logical_live_bytes']
        detail = mosaic_space(state)
        total = detail['data_bytes']
        retained = state.get('reader_retained_data_bytes', 0)
        holes = total - logical - retained
        assert 0 <= holes <= state['free_data_bytes']
        assert logical + state['free_data_bytes'] + retained == state['reserved_data_bytes']
        row = dict(logical_live_bytes=logical, ec_live_bytes=logical, live_buffer_bytes=0,
                   data_capacity_bytes=total, padding_bytes=0, hole_bytes=holes,
                   hole_count=state['holes'], invalid_data_bytes=0,
                   parity_bytes=detail['parity_bytes'],
                   retired_ec_catalog_bytes=0, gc_phase='none')
        assert state['retired_stripes'] == 0
    else:
        gc = state['gc']
        logical, total = gc['logical_live_bytes'], gc['total_data_bytes']
        live = total - gc['invalid_data_bytes'] - gc['live_padding_bytes']
        assert 0 <= live <= logical
        if scheme == 'foreground_regroup':
            assert gc['invalid_data_bytes'] == 0
        row = dict(logical_live_bytes=logical, ec_live_bytes=live, live_buffer_bytes=logical-live,
                   data_capacity_bytes=total, padding_bytes=gc['live_padding_bytes'],
                   hole_bytes=0, hole_count=0, invalid_data_bytes=gc['invalid_data_bytes'],
                   parity_bytes=gc['active_parity_bytes'],
                   retired_ec_catalog_bytes=gc['retired_ec_catalog_bytes'], gc_phase=gc['phase'])
    row['reader_retained_data_bytes'] = retained if scheme == 'mosaic' else 0
    assert row['data_capacity_bytes'] == sum(row[k] for k in
        ['ec_live_bytes', 'padding_bytes', 'hole_bytes', 'invalid_data_bytes', 'reader_retained_data_bytes'])
    row['logical_hole_bytes'] = state['free_data_bytes'] if scheme == 'mosaic' else 0
    row['excluded_never_written_bytes'] = detail['excluded_bytes'] if scheme == 'mosaic' else 0
    row['space_payload_scope'] = detail['scope'] if scheme == 'mosaic' else 'active_node_payload'
    payload = detail['payload_bytes'] if scheme == 'mosaic' else state['gc']['active_node_payload_bytes']
    ideal = (1 + state['m'] / state['k']) * logical
    row['active_node_payload_bytes'] = payload
    row['space_amplification'] = payload / ideal if ideal else 0
    overhead = row['padding_bytes'] + row['hole_bytes']
    row.update(padding_or_holes_bytes=overhead,
               padding_or_holes_percent_of_live=100 * overhead / logical if logical else 0,
               padding_or_holes_percent_of_data=100 * overhead / total if total else 0,
               mean_hole_bytes=row['logical_hole_bytes']/row['hole_count'] if row['hole_count'] else 0)
    return row


def write_csv(path, rows):
    with path.open('w') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def summarize(output):
    output = Path(output)
    rows, groups, peaks, amplification_peaks = [], {}, [], []
    for path in sorted(output.glob('r*/measurement.json')):
        assert json.loads(path.with_name('validation.json').read_text())['passed']
        run = json.loads(path.read_text()); metrics = run['metrics']
        peak = run['after'].get('gc', {}).get('space_peaks')
        amplification = run['after'].get('gc', {}).get('amplification_peak')
        if metrics['scheme'] == 'fastgc' and amplification is not None:
            assert amplification['scope'] == 'committed_active_node_payload_since_reset'
            assert amplification['defined']
            ratio = amplification['payload_bytes'] / ((1 + metrics['m']/metrics['k']) * amplification['logical_live_bytes'])
            amplification_peaks.append({**amplification, 'k': metrics['k'], 'm': metrics['m'],
                                        'repeat': metrics['repeat'], 'normalized_space_amplification': ratio})
        if metrics['scheme'] == 'fastgc' and peak is not None:
            assert peak['scope'] == 'committed_active_data_since_reset'
            assert peak['padding_bytes'] >= run['after']['gc']['live_padding_bytes']
            assert peak['padding_plus_invalid_bytes'] >= peak['padding_bytes']
            peaks.append(dict(k=metrics['k'], m=metrics['m'], scheme=metrics['scheme'],
                              repeat=metrics['repeat'], **peak,
                              padding_percent_of_live_at_peak=(100 * peak['padding_bytes'] / peak['live_bytes_at_padding_peak']
                                                               if peak['live_bytes_at_padding_peak'] else None)))
        for stage in ['before', 'after']:
            row = dict(k=metrics['k'], m=metrics['m'], scheme=metrics['scheme'],
                       repeat=metrics['repeat'], stage=stage, **snapshot(run[stage], metrics['scheme']))
            rows.append(row)
            groups.setdefault((row['k'], row['m'], row['scheme'], stage), []).append(row)
    assert rows, 'no validated benchmark snapshots'
    summary = []
    fields = [key for key in rows[0] if key not in ['k', 'm', 'scheme', 'repeat', 'stage', 'gc_phase', 'space_payload_scope']]
    for (k, m, scheme, stage), samples in sorted(groups.items()):
        assert len({row['space_payload_scope'] for row in samples}) == 1, 'cannot average mixed space accounting scopes'
        summary.append(dict(k=k, m=m, scheme=scheme, stage=stage, runs=len(samples),
                            space_payload_scope=';'.join(sorted({row['space_payload_scope'] for row in samples})),
                            **{field: mean(row[field] for row in samples) if all(row[field] is not None for row in samples) else None for field in fields},
                            overhead_bytes_min=min(row['padding_or_holes_bytes'] for row in samples),
                            overhead_bytes_max=max(row['padding_or_holes_bytes'] for row in samples)))
    # All schemes must account for the same logical workload at each boundary.
    for k, m, _, stage in groups:
        sizes = {row['logical_live_bytes'] for row in rows if (row['k'], row['m'], row['stage']) == (k, m, stage)}
        assert len(sizes) == 1
    write_csv(output/'space-per-run.csv', rows)
    write_csv(output/'space-summary.csv', summary)
    lines = [
        'Space accounting uses untimed before/after STATS snapshots from the update replay.',
        'When enabled, per-case space-trace.csv records committed samples during updates and a separate post-GC boundary.',
        'Summary values are means across completed runs at each boundary, not time averages or peaks.',
        'Final FastGC snapshots follow QUIESCE when traffic_gc_quiesced is true.',
        'Padding is active stored data minus invalid data and live EC value bytes; implicit zero padding is not resident payload.',
        'MosaicKV counts materialized chunks; unmaterialized data positions are virtual zeroes.',
        'Chunks remain counted after release. Each result records its accounting scope.',
        'All capacities are payload bytes, not process RSS, allocator/index metadata or transient physical memory.',
        'Space amplification = active accounted payload / ((1+m/k) * logical live bytes), including data, parity and buffer replicas.',
        'Overhead/live uses all live value bytes, including values in buffers. Parity, invalid and retired data are separate.',
        '', 'K+M | scheme | live KiB | padding KiB | holes KiB | hole count | overhead/live % | overhead/data % | invalid KiB | parity KiB | retired EC catalog KiB']
    for r in summary:
        if r['stage'] != 'after':
            continue
        lines.append(f"{r['k']}+{r['m']} | {r['scheme']} | {r['logical_live_bytes']/1024:.2f} | "
                     f"{r['padding_bytes']/1024:.2f} | {r['hole_bytes']/1024:.2f} | {r['hole_count']:.0f} | "
                     f"{r['padding_or_holes_percent_of_live']:.2f} | {r['padding_or_holes_percent_of_data']:.2f} | "
                     f"{r['invalid_data_bytes']/1024:.2f} | {r['parity_bytes']/1024:.2f} | {r['retired_ec_catalog_bytes']/1024:.2f}")
    if peaks:
        write_csv(output/'space-peaks.csv', peaks)
        lines += ['', 'FastGC committed active-data peaks cover updates and maintenance after preload reset.',
                  'Individual maxima may occur at different times; they exclude parity and transient copies.',
                  'K+M | repeat | padding peak MiB | invalid peak MiB | simultaneous padding+invalid peak MiB']
        for peak in peaks:
            lines.append(f"{peak['k']}+{peak['m']} | {peak['repeat']} | {peak['padding_bytes']/2**20:.6f} | "
                         f"{peak['invalid_data_bytes']/2**20:.6f} | {peak['padding_plus_invalid_bytes']/2**20:.6f}")
    if amplification_peaks:
        write_csv(output/'space-amplification-peaks.csv', amplification_peaks)
        lines += ['', 'FastGC amplification peak: committed active node payload / ((1+m/k) * logical live bytes) at the same commit.',
                  'Includes active data, parity and buffer replicas; excludes retired/uncommitted copies, key/index/slab overhead and proxy copies.']
        for peak in amplification_peaks:
            lines.append(f"{peak['k']}+{peak['m']} r{peak['repeat']}: peak={peak['normalized_space_amplification']:.9f}x; payload={peak['payload_bytes']}B; live={peak['logical_live_bytes']}B; observations={peak['observations']}")
    (output/'SPACE_REPORT.txt').write_text('\n'.join(lines)+'\n')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    summarize(args.output)
    print((args.output/'SPACE_REPORT.txt').read_text())
