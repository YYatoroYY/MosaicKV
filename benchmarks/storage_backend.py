"""Uniform experiment-wide storage backend selection and validation."""
import os


def selected_backend(args=None):
    backend = getattr(args, 'storage_backend', None) or os.environ.get('STORAGE_BACKEND', 'native')
    if backend not in ('memcached', 'native'):
        raise ValueError('invalid storage backend: ' + backend)
    return backend


def validate_storage(state, scheme=None, backend=None):
    backend = backend or selected_backend()
    assert state['storage_backend'] == backend, (state['storage_backend'], backend)
    for node in state['nodes']:
        assert node['healthy'] and node['parity'] is not None, node
        memory = node['parity']['memory']
        assert memory['storage_backend'] == backend
        assert memory[backend]['evictions'] == 0
        if backend == 'memcached':
            assert memory['memcached']['threads'] == 4 and not memory['parallel_io']
            if scheme is not None:
                assert (memory['batch_io'] is not None) == (scheme == 'mosaic')
        else:
            assert memory['memcached'] is None and memory['range_io']
            assert memory['memcached_rss_bytes'] == 0
            if scheme is not None:
                assert (memory['batch_io'] is not None) == (scheme == 'mosaic')
            if memory['batch_io'] is not None:
                assert memory['batch_io']['kind'] == 'native_ranges'
                assert memory['batch_io']['threads'] == int(os.environ.get('MOSAIC_IO_THREADS', '4'))
