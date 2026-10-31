#!/usr/bin/env python3
"""Capture the current host; run on each experiment machine and retain the output."""
import argparse
import json
import os
import platform
import subprocess
from pathlib import Path


def command(*args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=20).strip()
    except (OSError, subprocess.SubprocessError) as error:
        return {'unavailable': str(error)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = dict(platform=platform.platform(), machine=platform.machine(),
                  logical_cpus=os.cpu_count(), cpu=command('lscpu'),
                  memory=Path('/proc/meminfo').read_text(),
                  network=command('ip', '-brief', 'link'),
                  docker=command('docker', 'version'), compose=command('docker', 'compose', 'version'),
                  image=command('docker', 'image', 'inspect', os.environ.get('EC_IMAGE', 'mosaickv:local'),
                                '--format', '{{.Id}}'), python=platform.python_version())
    with args.output.open('x') as stream:
        json.dump(report, stream, indent=2)
        stream.write('\n')
