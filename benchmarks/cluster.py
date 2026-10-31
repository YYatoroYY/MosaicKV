"""Isolated local Docker clusters and benchmark utilities."""
import json
import math
import subprocess
import sys
from pathlib import Path
import zmq
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from integration import Cluster
from process_io import process_io
STRATEGIES = {name: name for name in ('mosaic', 'foreground_regroup', 'fastgc', 'inplace')}

def percentile(values, q):
    values = sorted(values)
    pos = (len(values) - 1) * q
    lower = math.floor(pos)
    return values[lower] + (values[math.ceil(pos)] - values[lower]) * (pos - lower)

def command(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()

class DirectCluster(Cluster):
    # Verification reuses node sockets and resolves containers once, outside timing.
    def connect_nodes(self):
        ids = self.compose('ps', '-q').split()
        self.containers = json.loads(command(['docker', 'inspect', *ids]))
        self.node_sockets = {}
        for c in self.containers:
            service = c['Config']['Labels']['com.docker.compose.service']
            if service.startswith('node'):
                node = int(service[4:])
                ip = next(iter(c['NetworkSettings']['Networks'].values()))['IPAddress']
                sock = self.context.socket(zmq.REQ)
                sock.linger = 0
                sock.sndtimeo = sock.rcvtimeo = 30000
                sock.connect(f'tcp://{ip}:7000')
                self.node_sockets[node] = sock

    def node_request(self, node, op, *args, status=b'OK'):
        sock = self.node_sockets[node]
        sock.send_multipart([op.encode(), str(node).encode(), *(v.encode() if isinstance(v, str) else v for v in args)])
        reply = sock.recv_multipart()
        assert len(reply) == 2 and reply[0] == status, reply
        return reply[1]
