#!/usr/bin/env python3
"""Render validated comparison CSVs as PNG/PDF figures without rerunning services."""
import argparse
import csv
import json
from pathlib import Path
from statistics import mean
from plot_space_trace import plot_traces

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

LABELS = {'mosaic': 'MosaicKV', 'foreground_regroup': 'KVMD', 'fastgc': 'FastGC', 'inplace': 'In-Place'}


def chart(directory, rows, field, label, name):
    groups = sorted({(int(r['k']), int(r['m'])) for r in rows})
    fig, axes = plt.subplots(1, len(groups), figsize=(5 * len(groups), 3.8), squeeze=False,
                             constrained_layout=True)
    for ax, (k, m) in zip(axes.flat, groups):
        selected = [r for r in rows if (int(r['k']), int(r['m'])) == (k, m)]
        selected.sort(key=lambda r: list(LABELS).index(r['scheme']))
        ax.bar([LABELS[r['scheme']] for r in selected], [float(r[field]) for r in selected])
        ax.set(title=f'{k}+{m}', ylabel=label)
        ax.tick_params(axis='x', labelrotation=20)
        ax.set_axisbelow(True)
        ax.grid(axis='y', alpha=.25)
    for extension in ('png', 'pdf'):
        fig.savefig(directory / f'{name}.{extension}', dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    args = parser.parse_args()
    directory = args.input
    if not json.loads((directory / 'manifest.json').read_text())['passed']:
        parser.error('only completed, validated runs can be plotted')
    with (directory / 'summary.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    for field, label, name in (
        ('mean_ms', 'Mean update latency (ms)', 'latency-mean'),
        ('p99_ms', 'P99 update latency (ms)', 'latency-p99'),
        ('ops_per_second', 'Single-client updates/s', 'throughput'),
        ('storage_rx_tx_plus_proxy_backend_tx_mib_per_run', 'Cumulative storage RX+TX + proxy TX (MiB)', 'traffic'),
        ('imbalance_lambda', 'Load imbalance lambda = (max - mean) / mean', 'balance'),
        ('cpu_ms_per_update', 'Service CPU time (ms/update)', 'cpu')):
        if field == 'cpu_ms_per_update' and any(r.get('instrumented_space_run') == 'True' for r in rows):
            continue  # Sampling CPU is not a clean per-update CPU measurement.
        chart(directory, rows, field, label, name)
    if list(directory.glob('r*/space-trace.csv')):
        plot_traces([directory], directory)
    with (directory / 'space-summary.csv').open() as stream:
        spaces = [r for r in csv.DictReader(stream) if r['stage'] == 'after']
    chart(directory, spaces, 'space_amplification',
          'Space amplification relative to ideal EC', 'space-amplification')
    fragments = {}
    for path in directory.glob('r*/validation.json'):
        value = json.loads(path.read_text()).get('mean_fragments')
        metrics = json.loads(path.with_name('measurement.json').read_text())['metrics']
        if value is not None:
            fragments.setdefault((metrics['k'], metrics['m'], metrics['scheme']), []).append(value)
    if fragments:
        records = [dict(k=k, m=m, scheme=s, mean_fragments=mean(v)) for (k,m,s),v in fragments.items()]
        with (directory / 'fragments-summary.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(records[0]))
            writer.writeheader(); writer.writerows(records)
        chart(directory, records, 'mean_fragments', 'Mean fragments per value', 'fragments')
    metadata = {}
    for path in directory.glob('r*/measurement.json'):
        metrics = json.loads(path.read_text())['metrics']
        value = metrics.get('metadata_final_bytes_per_key')
        if value is not None:
            metadata.setdefault((metrics['k'], metrics['m'], metrics['scheme']), []).append(value)
    if metadata:
        rows = [dict(k=k, m=m, scheme=s, bytes_per_key=mean(values))
                for (k, m, s), values in metadata.items()]
        with (directory / 'metadata-summary.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=['k', 'm', 'scheme', 'bytes_per_key'])
            writer.writeheader()
            writer.writerows(rows)
        chart(directory, rows, 'bytes_per_key', 'Useful catalog bytes/key (raw keys excluded)', 'metadata')


if __name__ == '__main__':
    main()
