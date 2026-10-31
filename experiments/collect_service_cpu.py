#!/usr/bin/env python3
"""Read Docker service CPU counters on a Linux host (also sent over SSH stdin)."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def read_counters(groups, root=Path('/sys/fs/cgroup')):
    """Support unified cgroup v2 and the Ubuntu 20.04 cpuacct v1 layout."""
    entries = [line.split(':', 2) for line in groups.splitlines()]
    unified = [path for _, controllers, path in entries if not controllers]
    if unified:
        path = root / unified[0].lstrip('/') / 'cpu.stat'
        values = dict(line.split() for line in path.read_text().splitlines())
        return str(path), {key: int(values[key]) for key in ('usage_usec', 'user_usec', 'system_usec')}
    for _, controllers, group in entries:
        if 'cpuacct' not in controllers.split(','):
            continue
        for mount in ('cpu,cpuacct', 'cpuacct,cpu', 'cpuacct'):
            path = root / mount / group.lstrip('/')
            if not (path / 'cpuacct.usage').is_file():
                continue
            ticks = dict(line.split() for line in (path / 'cpuacct.stat').read_text().splitlines())
            hz = os.sysconf('SC_CLK_TCK')
            return str(path), dict(usage_usec=int((path / 'cpuacct.usage').read_text()) // 1000,
                                   user_usec=int(ticks['user']) * 1000000 // hz,
                                   system_usec=int(ticks['system']) * 1000000 // hz)
    raise ValueError('Cannot locate service CPU cgroup; run on the Docker host with readable host cgroups')


def inspect(names):
    result = subprocess.run(['docker', 'inspect', *names], check=True, capture_output=True, text=True, timeout=15)
    return json.loads(result.stdout)


def identity(container):
    if not container['State']['Running'] or container['State']['Pid'] <= 0:
        raise ValueError('Service container is not running')
    return (container['Id'], container['State']['Pid'], container['State']['StartedAt'])


def collect(services):
    before = inspect(list(services.values()))
    if len(before) != len(services):
        raise ValueError('Incomplete Docker inspection')
    boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    rows = {}
    for (name, _), container in zip(services.items(), before):
        container_id, pid, started_at = identity(container)
        begin = time.monotonic_ns()
        path, counters = read_counters(Path(f'/proc/{pid}/cgroup').read_text())
        rows[name] = dict(container_id=container_id, cgroup_cpu_stat=path,
                          identity=[boot_id, container_id, pid, started_at],
                          host_start_monotonic_ns=begin, host_end_monotonic_ns=time.monotonic_ns(), **counters)
    after = inspect(list(services.values()))
    if [identity(c) for c in before] != [identity(c) for c in after]:
        raise ValueError('Service restarted during CPU snapshot')
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--containers', required=True, help='JSON mapping from service name to container name')
    args = parser.parse_args()
    print(json.dumps(collect(json.loads(args.containers))))
