"""Untimed catalog snapshots, including node-owned Mosaic hole indexes; excludes payload and RSS."""
import json
import math
import time


SCOPE = 'persistent_catalog_heap_usable_bytes'
USEFUL_SCOPE = 'persistent_catalog_useful_field_bytes_v1'
NET_SCOPE = USEFUL_SCOPE + '_minus_raw_keys'
RB_SCOPE = 'occupied_rb_tree_logical_field_bytes_v1'
INDEX_FIELDS = ('free_space_rb_tree', 'common_rb_tree', 'algorithm_total', 'rb_tree', 'gc_candidates', 'gc_candidates_rb_tree', 'space_counters', 'placement_counters')
FIELDS = tuple(f'metadata_{phase}_{unit}' for phase in ('preload', 'final')
               for unit in ('bytes', 'mib', 'bytes_per_key')) + ('metadata_delta_bytes', 'metadata_key_payload_bytes') + tuple(
                   f'metadata_{phase}_including_keys_{unit}' for phase in ('preload', 'final')
                   for unit in ('bytes', 'mib', 'bytes_per_key')) + tuple(
                       f'metadata_{phase}_{field}' for phase in ('preload', 'final')
                       for field in ('allocated_excluding_keys_bytes', 'allocated_excluding_keys_mib',
                                     'allocated_excluding_keys_bytes_per_key', 'useful_including_keys_bytes', 'unused_fragment_bytes',
                                     'reserved_fragment_bytes', 'active_fragment_bytes')) + tuple(
                       f'metadata_{phase}_{field}_{unit}' for phase in ('preload', 'final')
                       for field in INDEX_FIELDS for unit in ('bytes', 'mib', 'bytes_per_key'))


def fragment_usage(state, scheme):
    """Exact inline-slot accounting for the current compact mosaic layout."""
    if scheme != 'mosaic':
        return dict(unused_fragment_bytes=0, reserved_fragment_bytes=None, active_fragment_bytes=None)
    layout = state['metadata']
    # Optional descriptor detail cannot be inferred for mixed wide/heap
    # layouts. The actual allocated-heap comparison remains available.
    if not (layout.get('object_record_bytes') == 40 and layout.get('compact_fragment_bytes') == 8
            and layout.get('fragment_heap_slots') == 0 and state['fmax'] <= 4
            and 1 <= state['alignment_chunk'] <= 65536
            and state['allocation_failures'] == state['update_failures'] == 0
            and state['stripes'] < 2**32):
        return dict(unused_fragment_bytes=None, reserved_fragment_bytes=None, active_fragment_bytes=None)
    keys, fragments = state['keys'], state['fragments']
    assert 0 <= fragments <= 4 * keys
    reserved, active = 32 * keys, 8 * fragments
    return dict(unused_fragment_bytes=reserved - active,
                reserved_fragment_bytes=reserved, active_fragment_bytes=active)


def report_metrics(preload, final, key_payload_bytes, fragments=None):
    """Primary: used application fields minus raw keys. Allocation is auxiliary."""
    assert isinstance(key_payload_bytes, int) and key_payload_bytes >= 0
    assert preload['keys'] == final['keys']
    result = {'metadata_key_payload_bytes': key_payload_bytes}
    available = True
    for phase, record in (('preload', preload), ('final', final)):
        assert record['scope'] == SCOPE
        assert record['total_bytes'] == sum(record['groups'].values())
        assert record['groups']['objects'] >= key_payload_bytes
        gross = record['total_bytes']
        count = record['keys']
        usage = (fragments[phase] if fragments is not None else
                 dict(unused_fragment_bytes=None, reserved_fragment_bytes=None, active_fragment_bytes=None))
        result.update({f'metadata_{phase}_{field}': value for field, value in usage.items()})
        useful = record.get('useful')
        net = None
        if useful is not None:
            assert useful['scope'] == USEFUL_SCOPE and useful['keys'] == count
            assert useful['key_payload_bytes'] == key_payload_bytes
            assert useful['total_bytes'] == sum(useful['groups'].values())
            assert useful['groups']['objects'] >= key_payload_bytes
            net = useful['total_bytes'] - key_payload_bytes
        else:
            available = False  # Never infer useful fields from malloc occupancy.
        rb = record.get('rb_tree')
        if rb is not None:
            assert rb['scope'] == RB_SCOPE
            assert rb['total_bytes'] == sum(rb['groups'].values())
        hole_rb = rb['groups'].get('free_space') if rb else None
        gc_rb = rb['groups'].get('gc_candidates') if rb else None
        # Exclude general catalog trees; include algorithm-specific hole/GC trees.
        # Candidate IDs and hole ranges are already counted in net useful fields.
        specific_rb = hole_rb + gc_rb if hole_rb is not None and gc_rb is not None else None
        amounts = {
            'free_space_rb_tree': hole_rb,
            'common_rb_tree': rb['total_bytes'] - specific_rb if specific_rb is not None else None,
            'algorithm_total': net + specific_rb if net is not None and specific_rb is not None else None,
            'rb_tree': rb['total_bytes'] if rb else None,
            'gc_candidates_rb_tree': rb['groups'].get('gc_candidates') if rb else None,
            'gc_candidates': useful['groups'].get('gc_candidates') if useful else None,
            'space_counters': useful['groups'].get('space_counters') if useful else None,
            'placement_counters': useful['groups'].get('placement_counters') if useful else None,
        }
        for field, amount in amounts.items():
            result[f'metadata_{phase}_{field}_bytes'] = amount
            result[f'metadata_{phase}_{field}_mib'] = amount / 2**20 if amount is not None else None
            result[f'metadata_{phase}_{field}_bytes_per_key'] = (amount / count if count else 0) if amount is not None else None
        result[f'metadata_{phase}_useful_including_keys_bytes'] = useful['total_bytes'] if useful else None
        for label, amount in ((phase, net), (phase + '_including_keys', gross),
                              (phase + '_allocated_excluding_keys', gross - key_payload_bytes)):
            result[f'metadata_{label}_bytes'] = amount
            result[f'metadata_{label}_mib'] = amount / 2**20 if amount is not None else None
            result[f'metadata_{label}_bytes_per_key'] = (amount / count if count else 0) if amount is not None else None
    result['metadata_delta_bytes'] = (result['metadata_final_bytes'] - result['metadata_preload_bytes']) if available else None
    result['metadata_scope'] = NET_SCOPE if available else 'useful_metadata_unavailable'
    return result


def case_key_payload_bytes(case, preload_record):
    """Use the captured raw-key counter, otherwise count actual workload keys."""
    preload = json.loads((case / 'preload.json').read_text())
    assert preload_record['keys'] == preload['original_objects'] + preload['padding_objects']
    counter = preload['stats'].get('metadata', {}).get('key_bytes')
    if counter is not None:
        assert isinstance(counter, int) and counter >= 0
        return counter
    # Count UTF-8 key bytes from the replay file when the counter is absent.
    # Include each benchmark padding key once.
    with (case.parent / 'workload.json').open() as stream:
        workload = json.load(stream)
    initial = workload['initial_lengths']
    assert len(initial) == preload['original_objects']
    padding = preload['padding_lengths']
    assert len(padding) == preload['padding_objects'] and not set(padding).intersection(initial)
    return sum(len(key.encode('utf-8')) for key in initial) + sum(len(key.encode('utf-8')) for key in padding)


def snapshot(cluster, keys, phase):
    started = time.monotonic()
    with cluster.persistent_requests(timeout_ms=300000):
        record = json.loads(cluster.request('METADATA_MEMORY'))
    assert record['scope'] == SCOPE
    assert record['keys'] == keys
    assert record['total_bytes'] > 0
    assert all(isinstance(n, int) and n >= 0 for n in record['groups'].values())
    assert record['total_bytes'] == sum(record['groups'].values())
    # The C++ diagnostic emits its ratio with default stream precision.
    assert math.isclose(record['bytes_per_key'], record['total_bytes'] / keys, rel_tol=1e-5)
    record.update(phase=phase, collection_seconds=time.monotonic() - started,
                  collected_outside_measurement=True)
    return record


def metrics(preload, final):
    # Key payload bytes are required to report allocation excluding keys.
    key_bytes = preload.get('useful', {}).get('key_payload_bytes')
    result = report_metrics(preload, final, key_bytes or 0)
    if key_bytes is None:
        result['metadata_key_payload_bytes'] = None
        for phase in ('preload', 'final'):
            for unit in ('bytes', 'mib', 'bytes_per_key'):
                result[f'metadata_{phase}_allocated_excluding_keys_{unit}'] = None
    return result


def useful_metadata_row(row):
    """Normalize allocation totals and useful metadata fields by accounting scope."""
    row = dict(row)
    exact = row.get('metadata_scope') == NET_SCOPE
    for phase in ('preload', 'final'):
        gross = row.get(f'metadata_{phase}_including_keys_bytes')
        keys = row.get('metadata_key_payload_bytes')
        if gross not in (None, '') and keys not in (None, ''):
            allocated = int(gross) - int(keys)
            assert allocated >= 0
            row[f'metadata_{phase}_allocated_excluding_keys_bytes'] = allocated
            row[f'metadata_{phase}_allocated_excluding_keys_mib'] = allocated / 2**20
        if not exact:
            for unit in ('bytes', 'mib', 'bytes_per_key'):
                row[f'metadata_{phase}_{unit}'] = None
    if not exact:
        row['metadata_delta_bytes'] = None
        row['metadata_scope'] = 'useful_metadata_unavailable'
    return row
