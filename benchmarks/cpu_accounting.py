"""Read cumulative service CPU time from host cgroup v2; no storage RPCs."""
from pathlib import Path
import time


class CpuMeter:
    def __init__(self, cluster):
        self.services = {}
        for container in cluster.containers:
            service = container['Config']['Labels']['com.docker.compose.service']
            if service != 'proxy' and not service.startswith('node'):
                continue
            pid = container['State']['Pid']
            groups = Path(f'/proc/{pid}/cgroup').read_text().splitlines()
            unified = [line[3:] for line in groups if line.startswith('0::')]
            assert len(unified) == 1, 'CPU accounting requires cgroup v2'
            path = Path('/sys/fs/cgroup') / unified[0].lstrip('/') / 'cpu.stat'
            assert path.is_file(), path
            self.services[service] = (container['Id'], path)
        assert set(self.services) == {'proxy', *(f'node{i}' for i in range(cluster.k + cluster.m))}

    def snapshot(self):
        start = time.monotonic_ns()
        services = {}
        for name, (container_id, path) in sorted(self.services.items()):
            values = {key: int(value) for key, value in
                      (line.split() for line in path.read_text().splitlines())}
            services[name] = dict(container_id=container_id, cgroup_cpu_stat=str(path),
                                  **{key: values[key] for key in ('usage_usec', 'user_usec', 'system_usec')})
        end = time.monotonic_ns()
        return dict(start_monotonic_ns=start, end_monotonic_ns=end, services=services)


def cpu_delta(before, after):
    assert before['services'].keys() == after['services'].keys()
    rows = {}
    for name, first in before['services'].items():
        last = after['services'][name]
        assert first['container_id'] == last['container_id']
        assert first['cgroup_cpu_stat'] == last['cgroup_cpu_stat']
        if 'identity' in first or 'identity' in last:
            if first.get('identity') != last.get('identity'):
                raise ValueError(f'Service identity changed during CPU measurement: {name}')
        row = {key.removesuffix('_usec')+'_seconds': (last[key]-first[key])/1e6
               for key in ('usage_usec', 'user_usec', 'system_usec')}
        assert all(value >= 0 for value in row.values())
        rows[name] = row
    seconds = ((after['start_monotonic_ns']+after['end_monotonic_ns']) -
               (before['start_monotonic_ns']+before['end_monotonic_ns']))/2e9
    total = sum(row['usage_seconds'] for row in rows.values())
    return dict(total_seconds=total, proxy_seconds=rows['proxy']['usage_seconds'],
                storage_seconds=sum(row['usage_seconds'] for name, row in rows.items() if name != 'proxy'),
                user_seconds=sum(row['user_seconds'] for row in rows.values()),
                system_seconds=sum(row['system_seconds'] for row in rows.values()),
                wall_seconds=seconds, average_cores=total/seconds if seconds else 0, services=rows)


CPU_SCOPE = ('Cumulative cgroup-v2 usage_usec differences summed across Proxy and all storage containers; '
             'includes ec_proxy, ec_node, memcached, all their threads and other container tasks; '
             'excludes host benchmark client, Docker daemon and unrelated containers. '
             'CPU seconds sum time across cores, not wall time. User/system components are separately rounded kernel counters. '
             'Snapshots read host cpu.stat files sequentially without container exec or storage RPCs; read windows are saved. '
             'Foreground starts after pre-update STATS and ends after update workers join. '
             'FastGC tail extends through QUIESCE, including raw-sample checkpoint time; final STATS/reporting excluded.')
