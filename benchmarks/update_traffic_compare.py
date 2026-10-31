#!/usr/bin/env python3
"""Update latency and storage-node RX+TX for KVMD, fastgc and mosaic."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
import random
from pathlib import Path
from statistics import mean, pstdev
import subprocess

from prefilled_resize import ROOT, run, percentile
from space_overhead import summarize as summarize_space

FIELDS = ['received_bytes', 'sent_bytes', 'served_request_bytes', 'served_reply_bytes',
          'peer_request_bytes', 'peer_reply_bytes', 'served_requests', 'errors']
CATEGORIES = ('kvmd_regroup', 'fastgc_gc')
TRAFFIC_TOPOLOGY = 'simulated_one_machine_per_node_and_independent_proxy'
PRIMARY_TRAFFIC_METRIC = 'storage_node_rx_plus_tx_plus_proxy_backend_tx'
MEASUREMENT_MODE = 'combined_latency_and_traffic'
SEPARATE_MEASUREMENT_MODE = 'separate_latency_and_traffic_replays'


def link_deltas(result):
    """Count request+reply once at the RPC initiator, including every retry.

    rpc_links RX/TX names refer to the destination, like maintenance_traffic.
    Proxy and every storage node represent distinct simulated machines.
    """
    def snapshot(stats):
        totals = {}
        groups = [('proxy', stats['node_requests'])]
        if 'buffer_replication' in stats:
            groups.append(('proxy', stats['buffer_replication']))
        groups.extend((f'node{n["id"]}', n['parity']) for n in stats['nodes'])
        for source, group in groups:
            assert 'rpc_links' in group, 'missing link counters; rerun with the new image'
            for link in group['rpc_links']:
                key = source, f'node{link["node"]}'
                values = totals.setdefault(key, [0, 0, 0, 0])
                for i, field in enumerate(('received_bytes', 'sent_bytes', 'requests', 'replies')):
                    values[i] += link[field]
        return totals

    before, after = (snapshot(result[phase]) for phase in ('before', 'after'))
    rows = []
    for source, destination in sorted(before.keys() | after.keys()):
        key = source, destination
        delta = [b - a for a, b in zip(before.get(key, [0]*4), after.get(key, [0]*4))]
        assert min(delta) >= 0, 'RPC link counters reset'
        rows.append(dict(source=source, destination=destination,
                         cross_machine=source != destination,
                         request_bytes=delta[0], reply_bytes=delta[1],
                         total_bytes=delta[0] + delta[1], requests=delta[2], replies=delta[3]))
    return rows


def cross_machine_totals(result):
    links = [link for link in link_deltas(result) if link['cross_machine']]
    proxy = sum(link['total_bytes'] for link in links if link['source'] == 'proxy')
    peer = sum(link['total_bytes'] for link in links if link['source'] != 'proxy')
    categories = {category: sum(maintenance_deltas(result, n['id'])[f'{category}_total_bytes']
                               for n in result['after']['nodes']) for category in CATEGORIES}
    other = proxy + peer - sum(categories.values())
    assert other >= 0
    return dict(total_bytes=proxy + peer, proxy_storage_bytes=proxy,
                storage_storage_bytes=peer, other_bytes=other,
                **{f'{category}_bytes': value for category, value in categories.items()})


def audit_links(result):
    """Independent receiver counters detect missing or duplicated sender paths."""
    links = link_deltas(result)
    for before, after in zip(result['before']['nodes'], result['after']['nodes']):
        assert before['id'] == after['id'] and before['healthy'] and after['healthy']
        node = f'node{after["id"]}'
        for field, endpoint, link_field in (
                ('served_request_bytes', 'destination', 'request_bytes'),
                ('served_reply_bytes', 'destination', 'reply_bytes'),
                ('peer_request_bytes', 'source', 'request_bytes'),
                ('peer_reply_bytes', 'source', 'reply_bytes')):
            measured = after['parity']['traffic'][field] - before['parity']['traffic'][field]
            expected = sum(link[link_field] for link in links if link[endpoint] == node)
            assert measured == expected, (node, field, measured, expected)


def endpoint_totals(result):
    """Backend endpoint load, including the proxy but excluding the client link.

    A byte crossing a link has one sending and one receiving endpoint. Keep
    storage-only RX+TX as its own metric; do not selectively divide peer traffic.
    Sender link records use request/reply directions, not proxy RX/TX names.
    """
    links = [link for link in link_deltas(result) if link['cross_machine']]
    proxy_tx = sum(link['request_bytes'] for link in links if link['source'] == 'proxy')
    proxy_rx = sum(link['reply_bytes'] for link in links if link['source'] == 'proxy')
    storage_rx = sum(b['parity']['traffic']['received_bytes'] - a['parity']['traffic']['received_bytes']
                     for a, b in zip(result['before']['nodes'], result['after']['nodes']))
    storage_tx = sum(b['parity']['traffic']['sent_bytes'] - a['parity']['traffic']['sent_bytes']
                     for a, b in zip(result['before']['nodes'], result['after']['nodes']))
    proxy_links = proxy_rx + proxy_tx
    peer_links = sum(link['total_bytes'] for link in links if link['source'] != 'proxy')
    unique_links = proxy_links + peer_links
    storage_total = storage_rx + storage_tx
    backend_rx, backend_tx = storage_rx + proxy_rx, storage_tx + proxy_tx
    assert min(storage_rx, storage_tx, proxy_rx, proxy_tx) >= 0
    assert storage_total == proxy_links + 2 * peer_links
    assert backend_rx == backend_tx == unique_links
    return dict(storage_rx_bytes=storage_rx, storage_tx_bytes=storage_tx,
                storage_rx_tx_bytes=storage_total,
                storage_rx_tx_plus_proxy_backend_tx_bytes=storage_total + proxy_tx,
                proxy_backend_rx_bytes=proxy_rx, proxy_backend_tx_bytes=proxy_tx,
                proxy_backend_rx_tx_bytes=proxy_links,
                storage_peer_unique_bytes=peer_links, unique_backend_link_bytes=unique_links,
                all_backend_endpoint_rx_tx_bytes=backend_rx + backend_tx)


def maintenance_deltas(result, node):
    delta = {}
    for category in CATEGORIES:
        for field in ('received_bytes', 'sent_bytes', 'total_bytes'):
            value = 0
            if result['metrics']['scheme'] != 'mosaic':
                before, after = (result[phase]['maintenance_traffic'] for phase in ('before', 'after'))
                assert before['unit'] == after['unit'] == 'rpc_application_frame_bytes'
                a, b = before[category][node], after[category][node]
                assert a['node'] == b['node'] == node
                value = b[field] - a[field]
                assert value >= 0
            delta[f'{category}_{field}'] = value
    return delta


def node_deltas(result):
    rows = []
    duration = result['metrics'].get('traffic_seconds') or result['metrics']['operations'] / result['metrics']['ops_per_second']
    for a, b in zip(result['before']['nodes'], result['after']['nodes']):
        assert a['id'] == b['id'] and a['healthy'] and b['healthy']
        a, b, node = a['parity']['traffic'], b['parity']['traffic'], a['id']
        assert a['unit'] == b['unit'] == 'rpc_application_frame_bytes'
        delta = {field: b[field] - a[field] for field in FIELDS}
        assert all(v >= 0 for v in delta.values())
        if result['metrics']['scheme'] != 'fastgc':
            assert delta['errors'] == 0
        delta['total_bytes'] = delta['received_bytes'] + delta['sent_bytes']
        delta.update(maintenance_deltas(result, node))
        for field in ('received_bytes', 'sent_bytes', 'total_bytes'):
            delta[f'other_{field}'] = delta[field] - sum(delta[f'{category}_{field}'] for category in CATEGORIES)
            assert delta[f'other_{field}'] >= 0, 'maintenance must be a subset of total node traffic'
        delta['mib_per_second'] = delta['total_bytes'] / 2**20 / duration
        delta['snapshot_interval_seconds'] = (b['elapsed_ns'] - a['elapsed_ns']) / 1e9
        rows.append(dict(node=node, **delta))
    return rows


def balance(values):
    average = mean(values)
    deviation = pstdev(values)
    return dict(stddev_bytes=deviation, stddev_mib=deviation / 2**20,
                cv_percent=100 * deviation / average if average else 0,
                max_over_mean=max(values) / average if average else 0,
                imbalance_lambda=(max(values) - average) / average if average else 0,
                min_over_mean=min(values) / average if average else 0)


def audit(result):
    if 'cpu' in result:
        from cpu_accounting import cpu_delta
        cpu = result['cpu']
        total = cpu_delta(cpu['before'], cpu['after'])
        assert total == cpu['total']
        assert len(total['services']) == result['metrics']['k'] + result['metrics']['m'] + 1
        assert abs(total['total_seconds'] - cpu['foreground']['total_seconds'] - cpu['gc_tail']['total_seconds']) < 1e-7
        assert total['total_seconds'] == result['metrics']['cpu_total_seconds']
        assert abs(total['total_seconds'] - total['proxy_seconds'] - total['storage_seconds']) < 1e-7
        if result['metrics']['scheme'] != 'fastgc':
            assert cpu['gc_tail']['total_seconds'] == 0
    assert result['metrics']['traffic_enabled']
    assert len(result['samples']) == (result['metrics']['operations'] if result['metrics']['latency_collected'] else 0)
    assert result['after']['node_requests']['traffic_enabled']
    assert all(n['parity']['traffic']['enabled'] for n in result['after']['nodes'])
    audit_links(result)
    endpoint_totals(result)
    nodes = node_deltas(result)
    links = link_deltas(result)
    # Charge only storage endpoints: proxy links once, storage links twice.
    expected = sum(link['total_bytes'] * (1 if link['source'] == 'proxy' else 2)
                   for link in links)
    assert sum(n['total_bytes'] for n in nodes) == expected
    scheme = result['metrics']['scheme']
    assert all(w['resets'] == 0 for w in result['after']['node_requests']['workers'])
    if scheme == 'mosaic':
        assert result['after']['alignment_chunk'] == result['metrics'].get('alignment_chunk', 4096)
        assert result['after']['placement_policy'] == 'parity_bandwidth'
        assert result['after']['rebalance_every_updates'] == 1
        assert result['before']['committed_object_updates'] == 0
        assert result['after']['committed_object_updates'] == result['metrics']['operations']
        assert result['after']['rebalance_events'] == result['after']['resize_updates'] - result['before']['resize_updates']
    else:
        cleanup = result['metrics'].get('replica_cleanup', False)
        assert result['after']['temporary_replica_cleanup'] == cleanup
        if cleanup:
            if scheme == 'inplace':
                assert result['after']['buffer_delete_requests'] == result['before']['buffer_delete_requests']
            else:
                assert result['after']['buffer_delete_requests'] > result['before']['buffer_delete_requests']
            observed = sum(b['parity']['traffic']['operations'].get(op, {}).get('requests', 0) -
                           a['parity']['traffic']['operations'].get(op, {}).get('requests', 0)
                           for a, b in zip(result['before']['nodes'], result['after']['nodes'])
                           for op in ('BUFFER_DELETE', 'BUFFER_DELETE_BATCH'))
            assert observed == result['after']['buffer_delete_requests'] - result['before']['buffer_delete_requests']
        else:
            assert result['after']['buffer_delete_requests'] == 0
        assert result['flow']['gc']['failures'] == 0
    if scheme == 'inplace':
        if 'inplace_delta_updates' in result['after']:
            updates = result['metrics']['operations']
            assert result['flow']['inplace_delta_updates'] == updates
            assert result['flow']['inplace_reencodes'] == 0
            requests = {}
            for op in ('RMW_RESIZE', 'PATCH_RESIZE', 'READ', 'WRITE'):
                requests[op] = sum(
                    b['parity']['traffic']['operations'].get(op, {}).get('requests', 0) -
                    a['parity']['traffic']['operations'].get(op, {}).get('requests', 0)
                    for a, b in zip(result['before']['nodes'], result['after']['nodes']))
            assert requests['RMW_RESIZE'] == updates
            assert requests['PATCH_RESIZE'] == updates * result['metrics']['m']
            assert requests['READ'] == requests['WRITE'] == 0
        else:
            assert result['flow']['inplace_reencodes'] == result['metrics']['operations']
        assert result['flow']['reencoded_remainders'] == result['flow']['sealed_stripes'] == 0
        assert result['after']['gc']['cycles'] == result['after']['gc']['invalid_data_bytes'] == 0
    if scheme == 'fastgc':
        assert result['metrics']['traffic_gc_quiesced']
        assert result['after']['gc']['suspended'] and not result['after']['gc']['busy']
        peak = result['after']['gc'].get('amplification_peak')
        if peak is not None:
            assert peak['scope'] == 'committed_active_node_payload_since_reset' and peak['defined']
            assert peak['payload_bytes'] == peak['data_bytes'] + peak['parity_bytes'] + peak['buffer_replica_bytes']
            assert peak['observations'] == result['after']['gc']['space_peaks']['observations']
            for state in (result['before']['gc'], result['after']['gc']):
                assert peak['payload_bytes'] * state['logical_live_bytes'] >= state['active_node_payload_bytes'] * peak['logical_live_bytes']
        # A short count-mode run may never reach its trigger. A triggered pass
        # may also have no useful merge; neither case requires GC payload I/O.
        assert not result['before']['gc']['paused']
        every = result['metrics'].get('gc_every_updates', 0)
        if every:
            before, after = result['before']['gc'], result['after']['gc']
            assert before['every_updates'] == after['every_updates'] == every
            assert before['cycles'] == before['update_triggers'] == 0
            assert after['update_triggers'] == result['metrics']['operations'] // every
            # Older archived runs did not coalesce triggers.
            assert after['update_triggers'] == after['completed_update_rounds'] + after.get('coalesced_update_triggers', 0)
            if 'started_update_rounds' in after:
                assert after['started_update_rounds'] == after['completed_update_rounds']
            assert after['pending_update_rounds'] == 0 and after['drain_on_update_trigger']
            if after['update_triggers']:
                assert result['flow']['gc']['cycles'] > 0
            else:
                assert result['flow']['gc']['cycles'] == 0
                assert result['flow']['gc']['read_payload_bytes'] == result['flow']['gc']['write_payload_bytes'] == 0
    if scheme == 'foreground_regroup' and result['flow']['reencoded_remainders']:
        assert sum(n['kvmd_regroup_total_bytes'] for n in node_deltas(result)) > 0
    if scheme == 'fastgc' and result['flow']['gc']['read_payload_bytes']:
        assert sum(n['fastgc_gc_total_bytes'] for n in node_deltas(result)) > 0


def audit_latency(result):
    assert not result['metrics']['traffic_enabled'] and result['metrics']['latency_collected']
    assert len(result['samples']) == result['metrics']['operations']
    for phase in ('before', 'after'):
        stats = result[phase]
        groups = [stats['node_requests'], *(n['parity'] for n in stats['nodes'])]
        if 'buffer_replication' in stats:
            groups.append(stats['buffer_replication'])
        for group in groups:
            assert not group['traffic_enabled']
            assert all(link['total_bytes'] == link['requests'] == link['replies'] == 0 for link in group['rpc_links'])
        for node in stats['nodes']:
            traffic = node['parity']['traffic']
            assert not traffic['enabled']
            assert traffic['received_bytes'] == traffic['sent_bytes'] == traffic['served_requests'] == 0
        for category in CATEGORIES:
            assert all(item['total_bytes'] == 0 for item in stats.get('maintenance_traffic', {}).get(category, []))


def summarize(output):
    groups, node_rows, link_rows, audits = {}, [], [], {}
    for path in sorted(output.glob('r*/measurement.json')):
        assert json.loads(path.with_name('validation.json').read_text())['passed']
        r = json.loads(path.read_text()); audit(r)
        if r['metrics']['latency_collected']:
            latency_path, latency_run = path.parent, r.copy()
        else:
            latency_path = output / 'latency' / path.parent.name
            assert json.loads((latency_path / 'validation.json').read_text())['passed']
            latency_run = json.loads((latency_path / 'measurement.json').read_text())
            audit_latency(latency_run)
        for field in ('k', 'm', 'scheme', 'repeat', 'operations', 'clients'):
            assert latency_run['metrics'][field] == r['metrics'][field]
        r['latency_run'] = latency_run
        metrics = r['metrics']; nodes = node_deltas(r)
        b = balance([n['total_bytes'] for n in nodes])
        groups.setdefault((metrics['k'], metrics['m'], metrics['scheme']), []).append((r, nodes, b))
        for node in nodes:
            node_rows.append(dict(k=metrics['k'], m=metrics['m'], scheme=metrics['scheme'], repeat=metrics['repeat'], **node))
        for link in link_deltas(r):
            link_rows.append(dict(k=metrics['k'], m=metrics['m'], scheme=metrics['scheme'], repeat=metrics['repeat'], **link))
        audits[path.parent.name] = dict(traffic_topology=TRAFFIC_TOPOLOGY,
                                      primary_traffic_metric=PRIMARY_TRAFFIC_METRIC,
                                      measurement_mode=MEASUREMENT_MODE if r['metrics']['latency_collected'] else SEPARATE_MEASUREMENT_MODE,
                                      latency_run=str(latency_path.relative_to(output)),
                                      storage_node_bytes={field: sum(n[field] for n in nodes)
                                                          for field in ('received_bytes', 'sent_bytes', 'total_bytes')},
                                      cross_machine=cross_machine_totals(r),
                                      backend_endpoints=endpoint_totals(r),
                                      balance=b, flow=r['flow'], rebalance_events=r['after'].get('rebalance_events'),
                                      node_migrations=r['after'].get('node_migrations'),
                                      coefficient_changes=r['after'].get('coefficient_changes'))
    rows = []
    for (k, m, scheme), cases in sorted(groups.items()):
        samples = [s['latency_ms'] for r, _, _ in cases for s in r['latency_run']['samples']]
        seconds = sum(r['latency_run']['metrics']['operations'] / r['latency_run']['metrics']['ops_per_second'] for r, _, _ in cases)
        mib = mean(sum(n['total_bytes'] for n in nodes) / 2**20 for _, nodes, _ in cases)
        cross = [cross_machine_totals(r) for r, _, _ in cases]
        endpoints = [endpoint_totals(r) for r, _, _ in cases]
        cpu_metrics = [r['metrics'] for r, _, _ in cases if 'cpu_total_seconds' in r['metrics']]
        cross_mib = mean(c['total_bytes'] for c in cross) / 2**20
        rows.append(dict(k=k, m=m, scheme=scheme, operations=len(samples), mean_ms=mean(samples),
                         space_sample_every=cases[0][0]['metrics'].get('space_sample_every', 0),
                         instrumented_space_run=any(r['metrics'].get('instrumented_space_run', False) for r, _, _ in cases),
                         p50_ms=percentile(samples, .5), p95_ms=percentile(samples, .95), p99_ms=percentile(samples, .99),
                         ops_per_second=len(samples) / seconds, node_total_mib_per_run=mib,
                         cpu_total_seconds_per_run=mean(c['cpu_total_seconds'] for c in cpu_metrics) if cpu_metrics else None,
                         cpu_proxy_seconds_per_run=mean(c['cpu_proxy_seconds'] for c in cpu_metrics) if cpu_metrics else None,
                         cpu_storage_seconds_per_run=mean(c['cpu_storage_seconds'] for c in cpu_metrics) if cpu_metrics else None,
                         cpu_ms_per_update=mean(c['cpu_ms_per_update'] for c in cpu_metrics) if cpu_metrics else None,
                         node_received_mib_per_run=mean(sum(n['received_bytes'] for n in ns) for _, ns, _ in cases)/2**20,
                         node_sent_mib_per_run=mean(sum(n['sent_bytes'] for n in ns) for _, ns, _ in cases)/2**20,
                         node_kib_per_update=sum(n['total_bytes'] for _, ns, _ in cases for n in ns)/1024/len(samples),
                         cross_machine_mib_per_run=cross_mib,
                         cross_machine_kib_per_update=sum(c['total_bytes'] for c in cross) / 1024 / len(samples),
                         proxy_storage_mib_per_run=mean(c['proxy_storage_bytes'] for c in cross) / 2**20,
                         storage_storage_mib_per_run=mean(c['storage_storage_bytes'] for c in cross) / 2**20,
                         proxy_backend_received_mib_per_run=mean(e['proxy_backend_rx_bytes'] for e in endpoints) / 2**20,
                         proxy_backend_sent_mib_per_run=mean(e['proxy_backend_tx_bytes'] for e in endpoints) / 2**20,
                         storage_rx_tx_plus_proxy_backend_tx_mib_per_run=mean(e['storage_rx_tx_plus_proxy_backend_tx_bytes'] for e in endpoints) / 2**20,
                         storage_rx_tx_plus_proxy_backend_tx_kib_per_update=sum(e['storage_rx_tx_plus_proxy_backend_tx_bytes'] for e in endpoints) / 1024 / len(samples),
                         proxy_backend_rx_tx_mib_per_run=mean(e['proxy_backend_rx_tx_bytes'] for e in endpoints) / 2**20,
                         all_backend_endpoint_rx_tx_mib_per_run=mean(e['all_backend_endpoint_rx_tx_bytes'] for e in endpoints) / 2**20,
                         all_backend_endpoint_kib_per_update=sum(e['all_backend_endpoint_rx_tx_bytes'] for e in endpoints) / 1024 / len(samples),
                         cv_percent_mean=mean(b['cv_percent'] for _, _, b in cases),
                         cv_percent_min=min(b['cv_percent'] for _, _, b in cases),
                         cv_percent_max=max(b['cv_percent'] for _, _, b in cases),
                         stddev_mib_mean=mean(b['stddev_mib'] for _, _, b in cases),
                         received_stddev_mib=mean(balance([n['received_bytes'] for n in ns])['stddev_mib'] for _, ns, _ in cases),
                         sent_stddev_mib=mean(balance([n['sent_bytes'] for n in ns])['stddev_mib'] for _, ns, _ in cases),
                         max_over_mean=mean(b['max_over_mean'] for _, _, b in cases),
                         imbalance_lambda=mean(b['imbalance_lambda'] for _, _, b in cases),
                         received_cv_percent=mean(balance([n['received_bytes'] for n in ns])['cv_percent'] for _, ns, _ in cases),
                         sent_cv_percent=mean(balance([n['sent_bytes'] for n in ns])['cv_percent'] for _, ns, _ in cases),
                         kvmd_regroup_mib_per_run=mean(sum(n['kvmd_regroup_total_bytes'] for n in ns)/2**20 for _, ns, _ in cases),
                         fastgc_gc_mib_per_run=mean(sum(n['fastgc_gc_total_bytes'] for n in ns)/2**20 for _, ns, _ in cases),
                         other_mib_per_run=mean(sum(n['other_total_bytes'] for n in ns) for _, ns, _ in cases)/2**20,
                         cross_machine_other_mib_per_run=mean(c['other_bytes'] for c in cross)/2**20,
                         gc_tail_seconds_mean=mean(r['metrics'].get('gc_tail_seconds', 0) for r, _, _ in cases)))
    for name, values in [('summary.csv', rows), ('per-node.csv', node_rows), ('per-link.csv', link_rows)]:
        with (output / name).open('w') as file:
            writer = csv.DictWriter(file, fieldnames=list(values[0])); writer.writeheader(); writer.writerows(values)
    (output / 'audit.json').write_text(json.dumps(audits, indent=2))
    workload = json.loads((output / 'workload.json').read_text())
    repeats = len({n['repeat'] for n in node_rows})
    cleanup_modes = sorted({int(r['metrics'].get('replica_cleanup', False))
                            for (_, _, scheme), cases in groups.items() if scheme != 'mosaic'
                            for r, _, _ in cases})
    separate = not next(iter(groups.values()))[0][0]['metrics']['latency_collected']
    assert all((not r['metrics']['latency_collected']) == separate for cases in groups.values() for r, _, _ in cases)
    method = [
        'Latency and traffic use separate fresh-cluster replays of the same workload/configuration; EC_PROFILE=0 in both.',
        'Latency replay: EC_TRAFFIC=0; client timestamps are formatted after all clients finish.',
        'Traffic replay: EC_TRAFFIC=1; per-request latency sampling disabled. Its execution time is not reported as performance.',
        'GC scheduling may differ between replays; bytes and latency are not paired per individual request.',
    ] if separate else [
        'Latency and traffic are both enabled in the same replay: EC_TRAFFIC=1, client timestamp sampling on, EC_PROFILE=0.',
        'Requests/timestamp arrays are prepared before timing; formatting, percentiles and reports run after all clients finish.',
        'Measured latency includes traffic-counter overhead; use --separate-metrics to measure latency with EC_TRAFFIC=0.',
    ]
    meta = workload.get('metadata', {})
    sizes = [step['new_bytes'] for step in workload['updates']]
    workload_description = (f'Finite Zipf theta={meta["theta"]}, {meta["key_bytes"]}B keys; trace 32B-bin proportions with documented within-bin assumptions.'
                            if meta.get('kind') == 'trace_bins_zipf' else 'Key order and new lengths follow workload.json.')
    initial_sizes = sorted(set(workload['initial_lengths'].values()))
    lines = [f'Single client; {len(workload["initial_lengths"])} preloaded values of sizes {initial_sizes}B; identical {len(workload["updates"])} resize updates per run; new sizes {min(sizes)}..{max(sizes)}B.',
             workload_description,
             'Preload is padded to a multiple of K with identical non-updated objects in all schemes; '
             'padding counts and lengths are recorded in preload.json.',
             *method,
             f'{len({(n["k"], n["m"]) for n in node_rows})} K/M configurations; {repeats} fresh-cluster repeats, scheme order rotated/reversed.',
             'Simulated topology: each storage node is one machine; Proxy is another independent machine.',
             'KVMD, fastgc and mosaic use the same storage-node RX/TX counters, reconciled against sender links. Baseline TEMP_REPLICA_CLEANUP=' +
             ','.join(map(str, cleanup_modes)) + '; M replicas still written.',
             ('fastgc starts background drain-to-no-progress passes every ' +
              str(next((r['metrics'].get('gc_every_updates', 0) for cases in groups.values() for r, _, _ in cases if r['metrics']['scheme'] == 'fastgc'), 0)) +
              ' successful UPDATEs (0 means default threshold scheduling); bounded batches continue within each pass. '
              'mosaic ranks placement on every size-changing update using unified data-plus-parity load.'),
             'Load and preload validation keep traffic and mosaic write-load counters disabled; PHASE RESIZE enables all nodes before measurement.',
             'Comparison subtracts pre-update snapshots from post-update snapshots:',
             'Preload and untimed correctness reads are excluded. Diagnostic RPC bytes are excluded; any manual timed monitoring must be disclosed separately because it can affect latency.',
             'Count all application multipart frame bytes, including payload, commands, keys and replies; exclude TCP/IP/ZMQ framing.',
             'Node receive=served request+peer reply; node send=served reply+peer request. Peer retries are counted.',
             'STATS/MEMORY/PING/CONFIG/BACKEND/PROFILE/LAYOUT/PHASE queries are excluded. Local memcached TCP traffic is not included.',
             'Primary metric: storage-node RX+TX plus Proxy backend TX (Proxy to storage). Proxy backend RX and Client-Proxy traffic are excluded from this selected metric.',
             'Storage-only RX+TX remains separately reported for node load and balance; all-backend-endpoint RX+TX also includes Proxy backend RX and is a different metric.',
             'Proxy-storage bytes contribute once; storage-storage bytes contribute at both storage endpoints.',
             'Client-Proxy traffic is outside this storage-internal metric. In-node encoding/copy/memcached traffic is excluded.',
             'Unique cross-machine link totals remain auxiliary diagnostics, separate from the primary storage-node total.',
             'Also report Proxy backend RX+TX separately, excluding Client-Proxy communication. All-backend-endpoint RX+TX = storage-node RX+TX + Proxy backend RX+TX = 2 * unique link bytes.',
             'Conservation checks: storage RX+TX = proxy-storage link bytes + 2 * storage-storage link bytes; backend total RX = backend total TX = unique link bytes.',
             'mosaic includes data-node requests/replies and both endpoints of parity PATCH_BATCH traffic, plus any other storage RPCs.',
             'Total includes KVMD old-stripe regroup and fastgc background GC. Category bytes are subsets, never added a second time.',
             'KVMD category includes old shard reads, survivor data/parity writes and old EC DELETE; new-value buffer/seal traffic stays in other.',
             'InPlace resize preserves shard names/nodes/coefficients, writes changed data plus all parity, and performs no GC; its traffic is included in other.',
             'fastgc category includes reads/recovery, remote data/parity writes, CLONE control RPCs and retired/orphan DELETE, including failed/conflicting attempts.',
             'After updates fastgc QUIESCE drains the active pass, any coalesced pending pass and in-flight cleanup, then freezes GC for the final snapshot; no extra trigger is added.',
             'Latency/ops per second cover foreground updates only. Node MiB/s includes GC tail and final snapshot in the traffic interval.',
             'Raw foreground samples are checkpointed after timed updates; the traffic interval includes checkpoint writing.',
             'Storage-node load imbalance lambda=(max-mean)/mean uses cumulative RX+TX per memory node, averaged over repeats. CV, standard deviation and max/mean are auxiliary fields. Proxy is excluded.',
             'CPU totals are cumulative cgroup-v2 CPU seconds summed over Proxy and all storage containers, including memcached and all threads. Update CPU includes FastGC tail; preload CPU is separate. Host benchmark client and Docker daemon are excluded. Unavailable CPU values remain blank.',
             'Latency percentiles are pooled across repeats. Zero storage disk writes verified.',
             ('Value readback and independent RS checks skipped; only counters and process I/O checked.'
              if any(not json.loads(p.read_text()).get('readback_performed', True)
                     for p in output.glob('r*/validation.json')) else
              'All values and sampled independent RS parity verified.'),
             '', 'K+M | scheme | mean ms | p99 ms | ops/s | storage RX MiB | storage TX MiB | storage RX+TX MiB/run | Proxy backend TX MiB | storage RX+TX + Proxy TX MiB/run | regroup MiB | GC MiB | storage stddev MiB | storage CV % | storage max/mean | imbalance lambda']
    for r in rows:
        lines.append(f'{r["k"]}+{r["m"]} | {r["scheme"]} | {r["mean_ms"]:.3f} | {r["p99_ms"]:.3f} | '
                     f'{r["ops_per_second"]:.1f} | {r["node_received_mib_per_run"]:.3f} | {r["node_sent_mib_per_run"]:.3f} | {r["node_total_mib_per_run"]:.3f} | '
                     f'{r["proxy_backend_sent_mib_per_run"]:.3f} | {r["storage_rx_tx_plus_proxy_backend_tx_mib_per_run"]:.3f} | '
                     f'{r["kvmd_regroup_mib_per_run"]:.2f} | {r["fastgc_gc_mib_per_run"]:.2f} | '
                     f'{r["stddev_mib_mean"]:.3f} | {r["cv_percent_mean"]:.2f} | {r["max_over_mean"]:.3f} | {r["imbalance_lambda"]:.6f}')
    (output / 'REPORT.txt').write_text('\n'.join(lines) + '\n')
    print('\n'.join(lines), flush=True)
    plot(output, node_rows)
    summarize_space(output)


def plot(output, nodes):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    schemes = [s for s in ['foreground_regroup', 'fastgc', 'inplace', 'mosaic'] if any(n['scheme'] == s for n in nodes)]
    bar_width = .8 / len(schemes)
    configs = sorted({(n['k'], n['m']) for n in nodes})
    columns = min(2, len(configs)); height = (len(configs) + columns - 1) // columns
    fig, axes = plt.subplots(height, columns, figsize=(6.5 * columns, 4 * height), constrained_layout=True, squeeze=False)
    for ax, (k, m) in zip(axes.flat, configs):
        for j, scheme in enumerate(schemes):
            values = [mean(n['total_bytes'] for n in nodes if n['k'] == k and n['m'] == m and n['scheme'] == scheme and n['node'] == i)
                      / 2**20 for i in range(k + m)]
            ax.bar(np.arange(k + m) + (j - (len(schemes) - 1) / 2) * bar_width,
                   values, width=bar_width * .95, label=scheme)
        ax.set(title=f'{k}+{m}', xlabel='Storage node ID', ylabel='RX + TX application MiB / run', xticks=range(k + m))
        ax.grid(axis='y', alpha=.2); ax.set_axisbelow(True)
    axes[0, 0].legend()
    for ax in list(axes.flat)[len(configs):]:
        fig.delaxes(ax)
    workload = json.loads((output / 'workload.json').read_text())
    fig.suptitle(f'{len(workload["updates"])} single-client resize updates; mean of {len({n["repeat"] for n in nodes})} runs')
    fig.savefig(output / 'per-node-traffic.png', dpi=170)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workload-file', help='replay a generated workload.json with initial_lengths and updates')
    parser.add_argument('--keys', type=int, default=960)
    parser.add_argument('--operations', type=int, default=4096)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--seed', type=int, default=20261006)
    parser.add_argument('--configs', nargs='+', default=['6+3', '6+4', '10+4', '10+6'])
    parser.add_argument('--schemes', nargs='+', choices=['mosaic', 'foreground_regroup', 'fastgc', 'inplace'],
                        default=['mosaic', 'foreground_regroup', 'fastgc', 'inplace'])
    parser.add_argument('--resume', action='store_true', help='reuse only fully validated cases with identical source/image/workload')
    parser.add_argument('--skip-readback-validation', action='store_true',
                        help='skip preload/final GET, META and independent RS reads; retain counter and I/O checks')
    parser.add_argument('--record-metadata-memory', action='store_true',
                        help='record Proxy catalog heap after preload and updates/GC tail, outside measurement')
    parser.add_argument('--space-sample-every', type=int, default=0,
                        help='sample committed space every N updates; 0 disables instrumentation')
    parser.add_argument('--preloaded-compose',
                        help='take over the first case from an idle fully loaded cluster; requires stopped previous client')
    parser.add_argument('--separate-metrics', action='store_true',
                        help='opt in to separate latency (traffic off) and traffic (latency off) replays; default collects both together')
    parser.add_argument('--replica-cleanup', type=int, choices=[0, 1], default=1,
                        help='KVMD/fastgc temporary replica cleanup; enabled by default')
    parser.add_argument('--alignment-chunk', type=int, default=4096,
                        help='mosaic alignment chunk bytes')
    parser.add_argument('--fmax', type=int, default=4)
    parser.add_argument('--tau', type=int, default=8)
    parser.add_argument('--gc-every-updates', type=int, default=5000,
                        help='fastgc background drain every N successful UPDATEs (default: 5000); 0 uses threshold scheduling')
    parser.add_argument('--gc-tail-timeout-seconds', type=int, default=30,
                        help='client wait limit for fastgc final QUIESCE; does not bound or change GC work')
    parser.add_argument('--storage-backend', choices=['memcached', 'native'],
                        default=os.environ.get('STORAGE_BACKEND', 'native'),
                        help='one backend for all schemes and all nodes in this experiment')
    parser.add_argument('--native-memory-mb', type=int, default=int(os.environ.get('NATIVE_MEMORY_MB', '512')),
                        help='native payload capacity per node; metadata/allocator overhead excluded')
    parser.add_argument('--memcached-memory-mb', type=int,
                        default=int(os.environ.get('MEMCACHED_MEMORY_MB', '512')),
                        help='memcached capacity per storage node, identical across schemes')
    args = parser.parse_args()
    if args.space_sample_every < 0:
        parser.error('space sample interval must be nonnegative')
    if args.preloaded_compose:
        assert args.skip_readback_validation and not args.resume and not args.separate_metrics
    measurement_mode = SEPARATE_MEASUREMENT_MODE if args.separate_metrics else MEASUREMENT_MODE
    configs = [tuple(map(int, config.split('+'))) for config in args.configs]
    assert configs and len(set(configs)) == len(configs)
    assert len(args.schemes) == len(set(args.schemes))
    assert args.keys > 0 and args.operations > 0 and args.repeats > 0
    assert 64 <= args.alignment_chunk <= 12 * 1024 * 1024 and 0 <= args.gc_every_updates <= 1000000000
    assert 1 <= args.fmax <= 4096 and 0 <= args.tau <= 1048576
    assert args.memcached_memory_mb > 0 and args.native_memory_mb > 0
    assert 0 < args.gc_tail_timeout_seconds <= 86400
    assert all(k > 0 and m > 0 and k + m <= 32 for k, m in configs)
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=args.resume)
    if args.workload_file:
        raw = Path(args.workload_file).read_bytes()
    else:
        rng = random.Random(args.seed)
        sizes = {f'small:{i}': 256 for i in range(args.keys)}
        workload = dict(initial_lengths=sizes.copy(), updates=[])
        for _ in range(args.operations):
            key = f'small:{rng.randrange(args.keys)}'
            old = sizes[key]; length = old
            while length == old:
                length = rng.randint(128, 512)
            workload['updates'].append(dict(key=key, old_bytes=old, new_bytes=length))
            sizes[key] = length
        raw = json.dumps(workload).encode()
    workload = json.loads(raw)
    assert workload['initial_lengths'] and workload['updates']
    assert all(0 < len(key.encode()) <= 128 and isinstance(size, int) and 1 <= size <= 4096
               for key, size in workload['initial_lengths'].items())
    initial = {key: bytes([65 + i % 26]) * size for i, (key, size) in enumerate(workload['initial_lengths'].items())}
    values, trace = initial.copy(), []
    for step in workload['updates']:
        key = step['key']; old = len(values[key]); assert old == step['old_bytes']
        assert 1 <= step['new_bytes'] <= 4096 and old != step['new_bytes']
        value = (b'\xa5' if values[key][:1] != b'\xa5' else b'\x5a') * step['new_bytes']
        trace.append((key, old, value)); values[key] = value
    args.keys, args.operations, args.warmup = len(initial), len(trace), 0
    # Each run pads the preload to complete K-sized groups without changing the update trace.
    image = os.environ.setdefault('EC_IMAGE', 'mosaickv:local')
    files = sorted(p for p in (ROOT / 'src').glob('*') if p.is_file())
    raw_hashes = subprocess.check_output(['docker', 'run', '--rm', '--entrypoint', 'sha256sum', image,
                                         *['/build/src/' + p.name for p in files]], text=True)
    hashes = {line.split()[1].split('/')[-1]: line.split()[0] for line in raw_hashes.splitlines()}
    assert all(hashes[p.name] == hashlib.sha256(p.read_bytes()).hexdigest() for p in files)
    if args.resume:
        previous = json.loads((output / 'manifest.json').read_text())
        assert not previous['passed'], 'completed run needs no resume'
        assert (output / 'workload.json').read_bytes() == raw
        assert previous['source_sha256'] == hashes
        assert previous['traffic_topology'] == TRAFFIC_TOPOLOGY
        assert previous['measurement_mode'] == measurement_mode
        assert previous['image_id'] == subprocess.check_output(
            ['docker', 'image', 'inspect', image, '--format', '{{.Id}}'], text=True).strip()
        for name in ['keys', 'operations', 'repeats', 'seed', 'configs']:
            assert previous['arguments'][name] == getattr(args, name)
        assert previous['arguments'].get('replica_cleanup', 0) == args.replica_cleanup
        assert previous['arguments'].get('schemes', ['mosaic', 'foreground_regroup', 'fastgc']) == args.schemes
        assert previous['arguments'].get('alignment_chunk', 4096) == args.alignment_chunk
        assert previous['arguments'].get('fmax', 4) == args.fmax
        assert previous['arguments'].get('tau', 8) == args.tau
        assert previous['arguments'].get('gc_every_updates', 0) == args.gc_every_updates
        assert previous['arguments'].get('memcached_memory_mb', 512) == args.memcached_memory_mb
        assert previous['arguments'].get('storage_backend', 'memcached') == args.storage_backend
        assert previous['arguments'].get('native_memory_mb', 512) == args.native_memory_mb
        assert previous['arguments'].get('skip_readback_validation', False) == args.skip_readback_validation
        assert previous['arguments'].get('record_metadata_memory', False) == args.record_metadata_memory
        assert previous['arguments'].get('space_sample_every', 0) == args.space_sample_every
    else:
        (output / 'workload.json').write_bytes(raw)
    manifest = dict(started=datetime.now(timezone.utc).isoformat(), image=image, passed=False,
                    arguments={**vars(args), 'output': str(output)},
                    image_id=subprocess.check_output(['docker', 'image', 'inspect', image, '--format', '{{.Id}}'], text=True).strip(),
                    traffic_topology=TRAFFIC_TOPOLOGY, traffic_unit='rpc_application_frame_bytes',
                    primary_traffic_metric=PRIMARY_TRAFFIC_METRIC,
                    measurement_mode=measurement_mode,
                    source_sha256=hashes, workload_sha256=hashlib.sha256(raw).hexdigest(),
                    scripts_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in
                                    [Path(__file__), Path(__file__).with_name('prefilled_resize.py'),
                                     Path(__file__).with_name('small_object_resize.py'), Path(__file__).with_name('space_overhead.py'),
                                     Path(__file__).with_name('stats_snapshot.py'),
                                     Path(__file__).with_name('cluster.py'),
                                     Path(__file__).with_name('space_accounting.py'),
                                     Path(__file__).with_name('space_trace.py'),
                                     Path(__file__).with_name('storage_backend.py'),
                                     ROOT / 'configure.py', ROOT / 'tests' / 'process_io.py',
                                     Path(__file__).with_name('cpu_accounting.py'),
                                     Path(__file__).with_name('metadata_accounting.py'),
                                     ROOT / 'tests' / 'integration.py']})
    if not args.resume:
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    if args.resume:
        assert previous['scripts_sha256'] == manifest['scripts_sha256'], 'benchmark scripts changed; start a fresh run'
        previous.setdefault('continuations', []).append(dict(started=manifest['started'], scripts_sha256=manifest['scripts_sha256']))
        manifest = previous
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    preloaded_compose = args.preloaded_compose
    for repeat in range(1, args.repeats + 1):
        schemes = list(args.schemes) if repeat % 2 else list(reversed(args.schemes))
        for index, (k, m) in enumerate(configs):
            shift = index % len(schemes)
            for scheme in schemes[shift:] + schemes[:shift]:
                phases = [('latency', False), ('traffic', True)] if args.separate_metrics else [('combined', True)]
                for phase, enabled in phases:
                    phase_output = output / 'latency' if phase == 'latency' else output
                    phase_output.mkdir(exist_ok=True)
                    case = phase_output / f'r{repeat}-{k}+{m}-{scheme}'
                    check = audit if enabled else audit_latency
                    if args.resume and case.exists():
                        assert json.loads((case / 'validation.json').read_text())['passed']
                        check(json.loads((case / 'measurement.json').read_text()))
                        print(f'REUSE validated {phase} r{repeat} {k}+{m} {scheme}', flush=True)
                        continue
                    print(f'CASE {phase} r{repeat} {k}+{m} {scheme}', flush=True)
                    run(args, phase_output, repeat, k, m, scheme, initial, trace, values,
                        preloaded_compose=preloaded_compose,
                        traffic_enabled=enabled, collect_latency=phase != 'traffic',
                        include_maintenance_tail=True,
                        replica_cleanup=bool(args.replica_cleanup) if scheme != 'mosaic' else None,
                        proxy_env=({'MOSAIC_PLACEMENT': 'parity_bandwidth', 'MOSAIC_REBALANCE_EVERY': '1',
                                    'ALIGNMENT_CHUNK': str(args.alignment_chunk),
                                    'MOSAIC_FMAX': str(args.fmax), 'MOSAIC_TAU': str(args.tau)} if scheme == 'mosaic' else
                                   {'GC_EVERY_UPDATES': str(args.gc_every_updates)} if scheme == 'fastgc' else None))
                    preloaded_compose = None
                    check(json.loads((case / 'measurement.json').read_text()))
                r = json.loads((output / f'r{repeat}-{k}+{m}-{scheme}/measurement.json').read_text()); audit(r)
                nodes = node_deltas(r); b = balance([n['total_bytes'] for n in nodes])
                print(f'STORAGE RX+TX {sum(n["total_bytes"] for n in nodes)/2**20:.2f}MiB '
                      f'CV={b["cv_percent"]:.2f}% max/mean={b["max_over_mean"]:.3f}', flush=True)
    summarize(output)
    manifest.update(passed=True, finished=datetime.now(timezone.utc).isoformat())
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
