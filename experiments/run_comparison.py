#!/usr/bin/env python3
"""Run an isolated comparison on an existing workload, then render its results."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SCHEMES = ('mosaic', 'foreground_regroup', 'fastgc', 'inplace')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment', choices=['update', 'space', 'metadata', 'alignment'], default='update')
    parser.add_argument('--workload-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--configs', nargs='+', default=['6+3'])
    parser.add_argument('--schemes', nargs='+', choices=SCHEMES, default=list(SCHEMES))
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--alignment-chunk', type=int, default=4096)
    parser.add_argument('--alignment-chunks', nargs='+', type=int, default=[1024, 4096, 16384])
    parser.add_argument('--fmax', type=int, default=4)
    parser.add_argument('--tau', type=int, default=8)
    parser.add_argument('--gc-every-updates', type=int, default=5000)
    parser.add_argument('--native-memory-mb', type=int, default=512)
    parser.add_argument('--space-sample-every', type=int, default=None,
                        help='completed updates per space sample (space experiment: 1000; others: disabled)')
    args = parser.parse_args()
    if args.space_sample_every is None:
        args.space_sample_every = 1000 if args.experiment == 'space' else 0
    if args.space_sample_every < 0:
        parser.error('space sample interval must be nonnegative')
    workload = args.workload_file.resolve()
    if not workload.is_file():
        parser.error('workload file does not exist')
    if args.output.exists():
        parser.error('choose a new output directory')
    if args.repeats < 1 or args.native_memory_mb < 1:
        parser.error('repeats and native memory must be positive')
    chunks = args.alignment_chunks if args.experiment == 'alignment' else [args.alignment_chunk]
    if len(set(chunks)) != len(chunks) or any(not 64 <= c <= 12 * 1024 * 1024 for c in chunks):
        parser.error('alignment chunks must be unique and in 64..12582912 bytes')
    env = dict(os.environ, EC_IMAGE=os.environ.get('EC_IMAGE', 'mosaickv:local'))
    output = args.output.resolve()
    if args.experiment == 'alignment':
        output.mkdir(parents=True)
    for chunk in chunks:
        directory = output / f'chunk-{chunk}' if args.experiment == 'alignment' else output
        command = [sys.executable, str(ROOT / 'benchmarks/update_traffic_compare.py'),
                   '--workload-file', str(workload), '--output', str(directory),
                   '--schemes', *(['mosaic'] if args.experiment == 'alignment' else args.schemes),
                   '--configs', *args.configs, '--repeats', str(args.repeats),
                   '--storage-backend', 'native', '--native-memory-mb', str(args.native_memory_mb),
                   '--alignment-chunk', str(chunk), '--fmax', str(args.fmax), '--tau', str(args.tau),
                   '--gc-every-updates', str(args.gc_every_updates),
                   '--space-sample-every', str(args.space_sample_every)]
        if args.experiment == 'metadata':
            command.append('--record-metadata-memory')
        print('Running:', ' '.join(command), flush=True)
        subprocess.run(command, check=True, env=env)
        subprocess.run([sys.executable, str(ROOT / 'experiments/plot_results.py'),
                        '--input', str(directory)], check=True)
    (output / 'experiment.json').write_text(json.dumps(
        dict(experiment=args.experiment, passed=True, alignment_chunks=chunks,
             workload_file=str(workload)), indent=2) + '\n')


if __name__ == '__main__':
    main()
