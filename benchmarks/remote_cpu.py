"""Collect physical-cluster service CPU counters through host SSH sessions."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time


REMOTE_CPU_SCOPE = (
    'Sum of cumulative Docker service cgroup CPU deltas on all configured hosts, including all service threads '
    'and memcached when colocated in a node container; excludes the client, SSH collector and Docker daemon. '
    'CPU seconds sum across cores. Parallel host reads are not simultaneous; client read windows and '
    'host-local timestamps are saved. Foreground excludes preload and final validation; update total '
    'extends through FastGC QUIESCE. Idle/background service work during collection windows is included. '
    'cgroup v1 user/system counters have scheduler-tick resolution.')


class RemoteCpuMeter:
    def __init__(self, inventory, node_ids, timeout=30):
        data = json.loads(Path(inventory).read_text())
        self.services = data['services']
        expected = {'proxy', *(f'node{i}' for i in node_ids)}
        if set(self.services) != expected:
            raise ValueError(f'CPU inventory must cover exactly {sorted(expected)}')
        self.groups = {}
        seen = set()
        for name, entry in self.services.items():
            host, container = entry['host'], entry['container']
            if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.@:-]*', host):
                raise ValueError('Invalid SSH host; use a host or configured SSH alias')
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', container):
                raise ValueError('Invalid Docker container name')
            if (host, container) in seen:
                raise ValueError('CPU inventory counts a container more than once')
            seen.add((host, container))
            self.groups.setdefault(host, {})[name] = container
        if timeout <= 0:
            raise ValueError('CPU timeout must be positive')
        self.timeout = timeout
        self.source = (Path(__file__).resolve().parents[1] / 'experiments/collect_service_cpu.py').read_text()

    def _read_host(self, item):
        host, services = item
        args = ['python3', '-', '--containers', json.dumps(services)]
        command = ([sys.executable, *args[1:]] if host == 'local' else
                   ['ssh', '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={self.timeout}',
                    host, shlex.join(args)])
        begin = time.monotonic_ns()
        result = subprocess.run(command, input=self.source, capture_output=True, text=True,
                                timeout=self.timeout, check=True)
        end = time.monotonic_ns()
        rows = json.loads(result.stdout)
        if set(rows) != set(services):
            raise ValueError(f'Incomplete CPU snapshot from {host}')
        for row in rows.values():
            row['host'] = host
            row['identity'] = [host, *row['identity']]
            row['client_start_monotonic_ns'] = begin
            row['client_end_monotonic_ns'] = end
            if any(not isinstance(row.get(k), int) or row[k] < 0 for k in ('usage_usec', 'user_usec', 'system_usec')):
                raise ValueError(f'Invalid CPU counters from {host}')
        return rows

    def snapshot(self):
        begin = time.monotonic_ns()
        with ThreadPoolExecutor(max_workers=len(self.groups)) as pool:
            rows = list(pool.map(self._read_host, self.groups.items()))
        services = {name: value for group in rows for name, value in group.items()}
        identities = [tuple(row['identity'][1:]) for row in services.values()]
        if len(set(identities)) != len(identities):
            raise ValueError('CPU inventory resolves multiple services to the same container')
        return dict(start_monotonic_ns=begin, end_monotonic_ns=time.monotonic_ns(),
                    services=services)
