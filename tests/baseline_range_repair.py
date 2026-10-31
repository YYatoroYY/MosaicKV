#!/usr/bin/env python3
"""Baseline range GET/repair: physical byte bounds, real erasures and retained tails."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'benchmarks'))
from cluster import DirectCluster
import struct


def packed(*items):
    parts = [str(item).encode() if not isinstance(item, bytes) else item for item in items]
    return b''.join(struct.pack('!I', len(part)) + part for part in parts)


def totals(state):
    fields = ['full_read_bytes', 'full_write_bytes', 'range_read_bytes', 'range_write_bytes']
    return {f: sum(n['parity']['memory']['native'][f] for n in state['nodes']) for f in fields}



def run(strategy, backend, k, m):
    with tempfile.TemporaryDirectory(prefix='ec-baseline-range-') as tmp:
        c = DirectCluster(Path(tmp)/'compose.json', k, m, 4096, strategy=strategy, coding_unit='object')
        config = json.loads(c.file.read_text())
        for s in config['services'].values():
            s['environment'].update(STORAGE_BACKEND=backend, NODE_TIMEOUT_MS='10000')
        config['services']['proxy']['environment']['GC_EVERY_UPDATES'] = '1000000'
        c.file.write_text(json.dumps(config))
        measurements = []
        def meta(key): return json.loads(c.request('META', key))['extents'][0]
        def stats(): return totals(json.loads(c.request('STATS'))) if backend == 'native' else None
        def delta(a):
            b = stats()
            return {key:b[key]-a[key] for key in a} if a else None
        def measure(label, action, read_bytes=None):
            before = stats(); start = time.perf_counter_ns()
            value = action(); elapsed = time.perf_counter_ns()-start
            io = delta(before)
            if io:
                assert io['full_read_bytes'] == 0, (label, io)
                if read_bytes is not None: assert io['range_read_bytes'] == read_bytes, (label, io, read_bytes)
            measurements.append(dict(scenario=label, latency_ms=elapsed/1e6, io=io))
            return value
        def helper_bytes(e, target, length):
            return sum(sorted(min(s['stored_bytes'], length) for s in e['shards'] if s['index'] != target)[:e['data_blocks']])
        def restart(nodes):
            for n in nodes: c.compose('restart','--timeout','0',f'node{n}')
            for sock in c.node_sockets.values(): sock.close(linger=0)
            c.connect_nodes()
            for n in nodes: assert c.node_request(n, 'BACKEND') == backend.encode()
        try:
            c.compose('up','-d','--no-build','--wait');c.ready();c.connect_nodes()
            with c.persistent_requests(timeout_ms=120000):
                if strategy == 'fastgc': c.request('GC','QUIESCE')
                sizes = [1,7,17,256,4096,32]+[127+i for i in range(max(0,k-6))]
                expected = {f'v{i}':bytes((i*19+j*13)%256 for j in range(size)) for i,size in enumerate(sizes)}
                for key,value in expected.items(): c.request('PUT',key,value)
                key = 'v2'; e=meta(key); target=e['block_index']; node=e['shards'][target]['node']
                assert measure('healthy-small-get',lambda:c.request('GET',key),17)==expected[key]
                count=measure('forced-small-repair',lambda:int(c.request('REPAIR_BENCH',key,str(node))),helper_bytes(e,target,17))
                assert count==1
                # Malformed physical size is an erasure, not implicit zero padding.
                c.node_request(node,'WRITE',e['shards'][target]['block'],b'bad')
                assert measure('wrong-length-degraded-get',lambda:c.request('GET',key),helper_bytes(e,target,17))==expected[key]
                assert measure('wrong-length-repair',lambda:int(c.request('REPAIR',key)),helper_bytes(e,target,17))==1
                assert measure('healthy-repair-no-payload',lambda:int(c.request('REPAIR',key)),0)==0
                # Two actual storage losses require replacement helpers and repair both shards.
                other=e['shards'][1]['node']
                restart([node,other])
                assert measure('two-loss-degraded-get',lambda:c.request('GET',key))==expected[key]
                assert measure('two-loss-repair',lambda:int(c.request('REPAIR',key)))==2
                c.verify_parity(expected)
                parity=e['shards'][k]
                restart([parity['node']])
                assert measure('parity-node-repair',lambda:int(c.request('REPAIR_NODE',str(parity['node']))),
                               helper_bytes(e,k,parity['stored_bytes']))==1
                c.verify_parity(expected)
                if strategy == 'inplace':
                    key='v4'; old=expected[key]; expected[key]=b'new-prefix!'
                    c.request('UPDATE',key,expected[key]);e=meta(key); target=e['block_index'];node=e['shards'][target]['node']
                    assert e['shards'][target]['stored_bytes']==len(old)>len(expected[key])
                    assert measure('shrunk-healthy-get',lambda:c.request('GET',key),len(expected[key]))==expected[key]
                    restart([node])
                    assert measure('shrunk-degraded-get',lambda:c.request('GET',key),helper_bytes(e,target,len(expected[key])))==expected[key]
                    assert measure('shrunk-full-codeword-repair',lambda:int(c.request('REPAIR',key)),helper_bytes(e,target,len(old)))==1
                    actual=c.node_request(node,'READ',e['shards'][target]['block'])
                    assert actual==expected[key]+old[len(expected[key]):], 'retained encoded suffix lost'
                    c.verify_parity(expected)
                    # A subsequent growth/RMW must read the restored nonzero tail correctly.
                    expected[key]=b'G'*1000;c.request('UPDATE',key,expected[key]);c.verify_parity(expected)
                elif strategy == 'foreground_regroup':
                    expected['v4']=b'R'*111;c.request('UPDATE','v4',expected['v4'])
                    e=meta('v2');assert e['data_blocks']==k-1
                    target=e['block_index'];node=e['shards'][target]['node']
                    assert measure('reduced-k-forced-repair',lambda:int(c.request('REPAIR_BENCH','v2',str(node))),helper_bytes(e,target,17))==1
                    c.verify_parity(expected)
                else:
                    # Whole-source GC needs exactly K live objects across the
                    # two dirty stripes: K-1 from the first, one from the second.
                    for i in range(k):
                        expected[f'g{i}']=bytes([80+i])*64;c.request('PUT',f'g{i}',expected[f'g{i}'])
                    expected['v4']=b'R'*111;c.request('UPDATE','v4',expected['v4'])
                    for i in range(1,k):
                        expected[f'g{i}']=b'S'*91;c.request('UPDATE',f'g{i}',expected[f'g{i}'])
                    e=meta('v2'); target=e['block_index'];node=e['shards'][target]['node']
                    # Damage one source without deleting it (DELETE is a permanent tombstone).
                    c.node_request(node,'WRITE',e['shards'][target]['block'],b'bad')
                    before=json.loads(c.request('GC','STATUS'));io_before=stats();c.request('GC','RUN')
                    deadline=time.monotonic()+30
                    while True:
                        status=json.loads(c.request('GC','STATUS'))
                        if not status['busy'] and status['cycles']>before['cycles']: break
                        assert time.monotonic()<deadline,status
                        time.sleep(.05)
                    assert status['commits']>before['commits'] and status['failures']==before['failures'],status
                    c.request('GC','QUIESCE');io=delta(io_before)
                    if io: assert io['range_read_bytes']>0, 'GC did not exercise range recovery'
                    c.verify_parity(expected)
                    measurements.append(dict(scenario='gc-degraded-source',commits=status['commits']-before['commits'],io=io,passed=True))
                # Empty/one-byte codewords must not inherit the 64-byte ISA-L minimum.
                tiny={f't{i}':(b'' if i==0 else b'x') for i in range(k)}
                # Complete any pending buffers first; these keys are independent.
                for i in range(k): c.request('PUT',f'pad{i}',b'P')
                for key,value in tiny.items(): c.request('PUT',key,value)
                for key,value in tiny.items(): assert c.request('GET',key)==value
                e=meta('t1');target=e['block_index'];node=e['shards'][target]['node']
                assert measure('one-byte-forced-repair',lambda:int(c.request('REPAIR_BENCH','t1',str(node))),helper_bytes(e,target,1))==1
                # A fresh full stripe for deterministic >M erasures, all helpers nonzero.
                for i in range(2*k): c.request('PUT',f'loss{i}',b'L'*73)
                e=meta('loss0');target=e['block_index']; nodes=[s['node'] for s in e['shards'][:m+1]]
                # Select target among the erased data slots, independent of buffer alignment.
                nodes=list(dict.fromkeys([e['shards'][target]['node']]+nodes))[:m+1]
                restart(nodes)
                assert b'fewer than K' in c.request('GET','loss0',status=b'UNAVAILABLE')
                assert b'fewer than K' in c.request('REPAIR','loss0',status=b'UNAVAILABLE')
                assert c.node_request(e['shards'][target]['node'],'CHECK_RANGES',packed(e['shards'][target]['block'],73,0,73))==b'0'
                measurements.append(dict(scenario='insufficient-sources-no-publish',passed=True))
            return dict(passed=True, strategy=strategy,backend=backend,k=k,m=m,measurements=measurements)
        finally:
            for sock in getattr(c,'node_sockets',{}).values(): sock.close(linger=0)
            c.compose('down','--volumes','--timeout','0');c.context.term()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--backend',choices=['native','memcached'],default='native')
    p.add_argument('--strategy',choices=['inplace','fastgc','foreground_regroup'],required=True)
    p.add_argument('--k',type=int,default=6);p.add_argument('--m',type=int,default=3)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    result=run(a.strategy,a.backend,a.k,a.m);a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
