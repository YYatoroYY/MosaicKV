"""Check every storage process, including children of the node supervisor."""
import json


def process_io(cluster, service):
    script = '''for d in /proc/[0-9]*; do
      name=$(cat "$d/comm" 2>/dev/null) || continue
      case "$name" in
        ec_node|ec_proxy|memcached)
          echo "PROCESS ${d##*/} $name"
          cat "$d/io"
          ;;
      esac
    done'''
    result = {}
    current = None
    for line in cluster.compose('exec', '-T', service, 'sh', '-c', script).splitlines():
        if line.startswith('PROCESS '):
            _, pid, current = line.split()
            assert current not in result
            result[current] = {'pid': int(pid)}
        else:
            key, value = line.split(': ', 1)
            result[current][key] = int(value)
    config = json.loads(cluster.compose('config', '--format', 'json'))
    backend = config['services'][service]['environment'].get('STORAGE_BACKEND', 'memcached')
    expected = {'ec_proxy'} if service == 'proxy' else ({'ec_node'} if backend == 'native' else {'ec_node', 'memcached'})
    assert set(result) == expected, (service, result)
    assert all(p['write_bytes'] == 0 for p in result.values()), (service, result)
    return result
