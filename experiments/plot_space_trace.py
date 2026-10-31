#!/usr/bin/env python3
"""Plot sampled update progress; mark post-GC boundaries separately."""
import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def read_traces(directories):
    traces = []
    for directory in map(Path, directories):
        if not json.loads((directory / 'manifest.json').read_text())['passed']:
            raise ValueError(f'Run is incomplete: {directory}')
        files = ([directory / 'space-trace.csv'] if (directory / 'space-trace.csv').is_file()
                 else sorted(directory.glob('r*/space-trace.csv')))
        for path in files:
            if path.parent != directory and not json.loads((path.parent / 'validation.json').read_text())['passed']:
                raise ValueError(f'Run is incomplete: {path.parent}')
            with path.open(newline='', encoding='utf-8') as stream:
                rows = list(csv.DictReader(stream))
            if not rows or rows[0]['stage'] != 'initial' or not any(r['stage'] == 'foreground_end' for r in rows):
                raise ValueError(f'Missing space trace boundaries: {path}')
            traces.append((path, rows))
    return traces


def plot_traces(directories, output):
    traces = read_traces(directories)
    if not traces:
        raise ValueError('No space traces; enable --space-sample-every')
    groups = sorted({(int(rows[0]['k']), int(rows[0]['m'])) for _, rows in traces})
    fig, axes = plt.subplots(1, len(groups), figsize=(5 * len(groups), 3.8), squeeze=False,
                             constrained_layout=True)
    for ax, (k, m) in zip(axes.flat, groups):
        for path, rows in traces:
            if (int(rows[0]['k']), int(rows[0]['m'])) != (k, m):
                continue
            foreground = [r for r in rows if r['stage'] != 'post_gc']
            label = f"{rows[0]['scheme']} ({path.parent.name})"
            line, = ax.plot([100 * float(r['progress']) for r in foreground],
                            [float(r['space_amplification']) for r in foreground], label=label)
            for row in rows:
                if row['stage'] == 'post_gc':
                    ax.scatter([100], [float(row['space_amplification'])], marker='o',
                               facecolors='none', edgecolors=line.get_color(), label=label + ' post-GC')
        ax.set(title=f'{k}+{m}', xlabel='Completed updates (%)', ylabel='Space amplification relative to ideal EC')
        ax.grid(alpha=.25)
        ax.legend(fontsize=7)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    for extension in ('png', 'pdf'):
        fig.savefig(output / f'space-amplification-progress.{extension}', dpi=180)
    plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, nargs='+', required=True,
                        help='validated endpoint or local comparison result directories')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plot_traces(args.input, args.output)
