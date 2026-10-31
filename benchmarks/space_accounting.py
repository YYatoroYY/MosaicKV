"""Calculate Mosaic space usage from service snapshots."""


def mosaic_space(state):
    raw_data = state['reserved_data_bytes']
    raw_parity = state['active_parity_bytes']
    raw = raw_data + raw_parity
    if state.get('space_accounting_scope') == 'materialized_chunks':
        assert state['space_accounted_payload_bytes'] == raw
        assert state['space_accounted_data_bytes'] == raw_data
        assert state['space_accounted_parity_bytes'] == raw_parity
        return dict(data_bytes=raw_data, parity_bytes=raw_parity, payload_bytes=raw,
                    raw_payload_bytes=raw, excluded_bytes=0, scope='materialized_chunks')
    if 'space_accounted_payload_bytes' not in state:
        # Snapshots without usage counters account for the full reserved capacity.
        return dict(data_bytes=raw_data, parity_bytes=raw_parity, payload_bytes=raw,
                    raw_payload_bytes=raw, excluded_bytes=None,
                    scope='legacy_includes_never_written_chunks')
    assert state['space_accounting_scope'] == 'exclude_never_written_chunks'
    data = state['space_accounted_data_bytes']
    parity = state['space_accounted_parity_bytes']
    payload = state['space_accounted_payload_bytes']
    excluded = state['never_written_chunk_bytes']
    assert 0 <= data <= raw_data and 0 <= parity <= raw_parity
    assert payload == data + parity and raw == payload + excluded
    return dict(data_bytes=data, parity_bytes=parity, payload_bytes=payload,
                raw_payload_bytes=raw, excluded_bytes=excluded,
                scope=state['space_accounting_scope'])
