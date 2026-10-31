"""Validate diagnostic snapshots before any per-node field is consumed."""
import json


class StatsCollectionError(RuntimeError):
    def __init__(self, stage, detail, snapshot=None):
        super().__init__(f'{stage}: incomplete STATS: {detail}')
        self.snapshot = snapshot


def require_node_stats(state, stage):
    nodes = state.get('nodes')
    if not isinstance(nodes, list) or not nodes:
        raise StatsCollectionError(stage, 'missing node list', state)
    errors = []
    for node in nodes:
        info = node.get('parity')
        if not node.get('healthy') or not isinstance(info, dict):
            errors.append(f"node {node.get('id', '?')}: {node.get('error') or 'missing node statistics'} "
                          f"(stats_timeout_ms={node.get('stats_timeout_ms', 'unknown')})")
        elif not all(isinstance(info.get(field), dict) for field in ('traffic', 'memory')):
            errors.append(f"node {node.get('id', '?')}: missing traffic/memory statistics")
    if state.get('stats_complete') is False and not errors:
        errors.append('proxy marked snapshot incomplete')
    if errors:
        raise StatsCollectionError(stage, '; '.join(errors), state)
    return state


def read_stats(cluster, stage, directory=None):
    try:
        state = json.loads(cluster.request('STATS'))
    except Exception as error:
        raise StatsCollectionError(stage, f'proxy query failed: {error}') from error
    if directory is not None:
        # Preserve the actual response, including errors, before validation.
        path = directory / f'stats-{stage}.json'
        temporary = path.with_suffix('.json.tmp')
        temporary.write_text(json.dumps(state))
        temporary.replace(path)
    return require_node_stats(state, stage)
