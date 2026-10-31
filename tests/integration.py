#!/usr/bin/env python3
"""Real Docker fault tests, isolated temporary projects/volumes. Requires pyzmq."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import itertools
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import threading
import struct
import uuid
import zlib

import zmq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from configure import generate


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RequestSession:
    """One thread-owned REQ socket; transport failures reset, never replay."""
    def __init__(self, context, endpoint, timeout_ms=30000):
        self.context, self.endpoint, self.timeout_ms = context, endpoint, timeout_ms
        self.socket = None
        self.connections = 0
        self.requests = 0

    def close(self):
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

    def exchange(self, frames):
        try:
            if self.socket is None:
                self.socket = self.context.socket(zmq.REQ)
                self.socket.linger = 0
                self.socket.sndtimeo = self.socket.rcvtimeo = self.timeout_ms
                self.socket.connect(self.endpoint)
                self.connections += 1
            self.socket.send_multipart(frames)
            self.requests += 1
            reply = self.socket.recv_multipart()
            if len(reply) != 2:
                raise AssertionError(('invalid response frame count', reply))
            return reply
        except BaseException:
            # A timed-out REQ cannot send again. Discard it; let the caller
            # handle this failure and create a socket only for the NEXT call.
            self.close()
            raise


class Cluster:
    def __init__(self, path, k, m, block_size, strategy="rmw", coding_unit="block"):
        self.port = free_port()
        self.k, self.m, self.block_size = k, m, block_size
        # Protocol/fault tests exercise enabled counters from their first RPC.
        # Experiment runners override this to load and explicitly start resize.
        self.file = generate(k, m, block_size, k + m, f"ec-kv-test-{os.getpid()}-{k}-{m}", self.port, path, strategy, coding_unit,
                             workload_phase="resize")
        self.context = zmq.Context()
        self._request_local = threading.local()

    def compose(self, *args):
        result = subprocess.run(["docker", "compose", "-f", str(self.file), *args],
                                capture_output=True, text=True, timeout=90)
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        return result.stdout

    @contextmanager
    def persistent_requests(self, timeout_ms=30000):
        """Reuse requests within this scope, closing before context teardown.

        Nested scopes on the same thread share one connection. Other threads
        keep their own sockets; formal measured UPDATE clients are unaffected.
        """
        existing = getattr(self._request_local, 'session', None)
        if existing is not None:
            yield existing
            return
        session = RequestSession(self.context, f'tcp://127.0.0.1:{self.port}', timeout_ms)
        self._request_local.session = session
        try:
            yield session
        finally:
            self._request_local.session = None
            session.close()

    def request(self, op, *args, status=b"OK"):
        frames = [op.encode(), *(x.encode() if isinstance(x, str) else x for x in args)]
        session = getattr(self._request_local, 'session', None)
        if session is None:
            with self.persistent_requests() as single:
                reply = single.exchange(frames)
        else:
            reply = session.exchange(frames)
        # A complete application error still leaves REQ ready for the next call.
        assert reply[0] == status, (op, reply[0], reply[1][:200])
        return reply[1]

    def ready(self):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                stats = json.loads(self.request("STATS"))
                if all(n["healthy"] for n in stats["nodes"]):
                    return stats
            except (zmq.ZMQError, AssertionError):
                pass
            time.sleep(0.2)
        raise AssertionError("cluster not ready")

    def node_request(self, node, op, *args, status=b"OK"):
        container = self.compose("ps", "-q", f"node{node}").strip()
        address = subprocess.check_output(["docker", "inspect", "--format",
                                          "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container], text=True).strip()
        with self.context.socket(zmq.REQ) as sock:
            sock.linger = 0
            sock.sndtimeo = sock.rcvtimeo = 15000
            sock.connect(f"tcp://{address}:7000")
            sock.send_multipart([op.encode(), str(node).encode(), *(x.encode() if isinstance(x, str) else x for x in args)])
            reply = sock.recv_multipart()
            assert len(reply) == 2 and reply[0] == status, (op, reply)
            return reply[1]

    def verify_parity(self, expected):
        def multiply(a, b):
            out = 0
            while b:
                if b & 1:
                    out ^= a
                a <<= 1
                if a & 256:
                    a ^= 0x11D
                b >>= 1
            return out
        def inverse(a):
            result = 1
            for _ in range(254):
                result = multiply(result, a)
            return result
        stripes = {}
        for key, data in expected.items():
            assert self.request("GET", key) == data
            for extent in json.loads(self.request("META", key))["extents"]:
                if extent["storage"] == "ec":
                    stripes[extent["stripe"]] = extent
        for extent in stripes.values():
            shards, actual_k = extent["shards"], extent["data_blocks"]
            blocks = [self.node_request(shard["node"], "READ", shard["block"]) for shard in shards]
            for i, shard in enumerate(shards):
                assert len(blocks[i]) == shard.get('stored_bytes', extent['block_bytes'])
                blocks[i] = blocks[i].ljust(extent['block_bytes'], b'\0')
            for j in range(self.m):
                parity = bytearray(extent["block_bytes"])
                assert all(len(block) == extent["block_bytes"] for block in blocks)
                for i in range(actual_k):
                    coefficient = inverse((actual_k + j) ^ i)
                    table = bytes(multiply(coefficient, x) for x in range(256))
                    scaled = blocks[i].translate(table)
                    for offset, byte in enumerate(scaled):
                        parity[offset] ^= byte
                assert bytes(parity) == blocks[actual_k + j], "RMW parity differs from independent full encode"

    def interrupted_rmw(self, expected):
        # Inject the exact persisted intent and perform a prefix of a multi-block RMW.
        # This deterministically exercises partial parity success and lost acknowledgments.
        key = "cross"
        value = bytes(x ^ 0x51 for x in expected[key])
        meta = json.loads(self.request("META", key))
        base = self.compose("exec", "-T", "proxy", "cat", "/meta/catalog").encode()
        tx = uuid.uuid4().hex
        def pack(fields):
            payload = b"".join(struct.pack(">I", len(field)) + field for field in fields)
            return str(zlib.crc32(payload)).encode() + b"\n" + payload
        intent = pack([b"RMW1", tx.encode(), key.encode(), value, b"UPDATE", base])
        command = ["docker", "compose", "-f", str(self.file), "exec", "-T", "proxy",
                   "sh", "-c", "cat > /meta/pending-rmw"]
        subprocess.run(command, input=intent, check=True, capture_output=True, timeout=15)
        lines = base.decode().splitlines()
        count = int(lines[1].split()[6])
        checksum_offset = 4 if lines[1].split()[0] == "ECKV3" else 3
        checksums = {}
        for line in lines[2:2 + count]:
            fields = line.split()
            offset = 5 + int(fields[3]) if lines[1].split()[0] in ("ECKV4", "ECKV5") else checksum_offset
            checksums[fields[0]] = list(map(int, fields[offset:]))
        extent = meta["extents"][0]
        assert extent["offset"] == 0 and extent["length"] >= 2 * self.block_size
        shards, sums = extent["shards"], checksums[extent["stripe"]]
        def request(index):
            header = f"{index} 0 {sums[index]} {self.k} {self.m} {self.block_size}\n"
            for j in range(self.m):
                shard = shards[self.k + j]
                header += f"{shard['node']} {shard['block']} {sums[self.k + j]}\n"
            return (f"{tx}_{index}", shards[index]["block"], header,
                    value[index * self.block_size:(index + 1) * self.block_size])
        first_request = request(0)
        result = self.node_request(shards[0]["node"], "RMW", *first_request)
        new_sums = list(map(int, result.split()))
        sums[0], sums[self.k:] = new_sums[0], new_sums[1:]
        failed = shards[-1]["node"]
        self.compose("stop", "-t", "0", f"node{failed}")
        second_request = request(1)
        self.node_request(shards[1]["node"], "RMW", *second_request, status=b"UNAVAILABLE")
        assert json.loads(self.request("STATS"))["pending_update"]
        self.request("GET", key, status=b"UNAVAILABLE")
        self.compose("start", f"node{failed}")
        self.compose("restart", "-t", "0", f"node{shards[1]['node']}", "proxy")
        self.ready()
        assert self.request("GET", key) == value
        assert not json.loads(self.request("STATS"))["pending_update"]
        expected[key] = value
        # Replaying the first DONE transaction after later updates must NOT restore old parity.
        assert self.node_request(shards[0]["node"], "RMW", *first_request) == result
        assert self.request("GET", key) == value
        print(f"PASS interrupted {self.k}+{self.m} RMW: partial fan-out, node/proxy restart and idempotent replay", flush=True)

    def check(self):
        stats = self.ready()
        assert (stats["k"], stats["m"], stats["block_size"]) == (self.k, self.m, self.block_size)
        expected = {"a": b"a" * 17, "b": b"b" * 19}
        for key, value in expected.items():
            self.request("PUT", key, value)
        a = json.loads(self.request("META", "a"))["extents"][0]
        b = json.loads(self.request("META", "b"))["extents"][0]
        assert a["stripe"] == b["stripe"] and a["offset"] == 0 and b["offset"] == 17
        assert a["shards"] == b["shards"] and b["block_index"] == 0
        assert json.loads(self.request("STATS"))["stripes"] == 1
        # Fill the stripe exactly, then cross block and stripe boundaries.
        expected["fill"] = bytes((i % 251 for i in range(self.k * self.block_size - 36)))
        expected["cross"] = bytes(range(256)) * ((self.k * self.block_size) // 256 + 1)
        expected["empty"] = b""
        expected["binary"] = b"\x00\xff\r\n" * 83
        for key in ("fill", "cross", "empty", "binary"):
            self.request("PUT", key, expected[key])
        workers_before = json.loads(self.request("STATS"))["nodes"]
        for node in workers_before:
            assert node["parity"]["parity_workers"] == self.m
        placement = json.loads(self.request("META", "a"))["extents"]
        stripes_before = json.loads(self.request("STATS"))["stripes"]
        for value in (b"L" * 17, b"X" * 17, b"F" * 17, b"F" * 17):
            self.request("UPDATE", "a", value)
            expected["a"] = value
            for key, data in expected.items():
                assert self.request("GET", key) == data
        assert json.loads(self.request("META", "a"))["extents"] == placement
        assert json.loads(self.request("STATS"))["stripes"] == stripes_before
        for op in ("UPDATE", "PUT"):
            self.request(op, "a", b"resize", status=b"INVALID")
        self.request("PUT", "a", b"Z" * 17)
        expected["a"] = b"Z" * 17
        # Non-aligned offsets, multiple data blocks and multiple stripes must update in place.
        for key in ("fill", "cross", "binary", "empty"):
            before = json.loads(self.request("META", key))["extents"]
            expected[key] = bytes(x ^ 0xA7 for x in expected[key])
            self.request("UPDATE", key, expected[key])
            assert json.loads(self.request("META", key))["extents"] == before
        self.verify_parity(expected)
        workers_after = json.loads(self.request("STATS"))["nodes"]
        for old, new in zip(workers_before, workers_after):
            assert [w["tid"] for w in old["parity"]["workers"]] == [w["tid"] for w in new["parity"]["workers"]]
        assert all(w["tasks"] >= 5 for w in workers_after[0]["parity"]["workers"])
        self.request("UPDATE", "absent", b"x", status=b"NOT_FOUND")
        self.request("PUT", b"", b"x", status=b"INVALID")
        self.request("GET", "absent", status=b"NOT_FOUND")
        self.request("WHAT", status=b"INVALID")
        self.request("PUT", "missing-value", status=b"INVALID")
        self.request("PING", *([b"x"] * 9), status=b"INVALID")
        self.request("PING") # Bad requests must not poison the REP socket.
        def concurrent(index):
            key, data = f"parallel-{index}", bytes([index]) * 29
            self.request("PUT", key, data)
            assert self.request("GET", key) == data
            return key, data
        with ThreadPoolExecutor(max_workers=8) as pool:
            expected.update(pool.map(concurrent, range(8)))
        # Every maximal erasure combination for the small configuration.
        combos = list(itertools.combinations(range(self.k + self.m), self.m)) if self.k == 2 else [(0, 2, 7)]
        for combo in combos:
            names = [f"node{i}" for i in combo]
            self.compose("stop", "-t", "0", *names)
            assert self.request("GET", "cross") == expected["cross"]
            self.compose("start", *names)
            self.ready()
        self.compose("stop", "-t", "0", "node0")
        self.request("UPDATE", "a", b"N" * 17, status=b"UNAVAILABLE")
        for key, data in expected.items():
            assert self.request("GET", key) == data
        self.compose("start", "node0")
        self.ready()
        # More than M erasures must return an error, never truncated/wrong data.
        names = [f"node{i}" for i in range(self.m + 1)]
        self.compose("stop", "-t", "0", *names)
        self.request("GET", "cross", status=b"UNAVAILABLE")
        self.compose("start", *names)
        self.ready()
        # Corrupt one real block, verify checksum-driven degraded read and repair.
        extent = json.loads(self.request("META", "b"))["extents"][0]
        shard = extent["shards"][0]
        name, block = f"node{shard['node']}", shard["block"]
        self.compose("exec", "-T", name, "sh", "-c", 'printf bad > "/data/$1"', "sh", block)
        assert self.request("GET", "b") == expected["b"]
        assert int(self.request("REPAIR", "b")) == 1
        assert int(self.request("REPAIR", "b")) == 0
        # Lose every shard on one node, then rebuild into its empty running volume.
        self.compose("exec", "-T", "node0", "sh", "-c", "find /data -maxdepth 1 -type f -name '*_*' -delete")
        assert self.request("GET", "cross") == expected["cross"]
        assert int(self.request("REPAIR_NODE", "0")) > 0
        self.interrupted_rmw(expected)
        self.verify_parity(expected)
        self.request("DELETE", "binary")
        del expected["binary"]
        self.request("GET", "binary", status=b"NOT_FOUND")
        # Recreate all containers, retaining only their independent named volumes.
        self.compose("down", "--timeout", "0")
        self.compose("up", "-d", "--no-build", "--wait")
        self.ready()
        for key, data in expected.items():
            assert self.request("GET", key) == data
        self.request("GET", "binary", status=b"NOT_FOUND")
        self.request("PUT", "after-restart", b"append")
        for key, data in expected.items():
            assert self.request("GET", key) == data
        print(f"PASS {self.k}+{self.m}, block={self.block_size}: packing, fixed-size RMW, worker reuse, parity oracle, interrupted replay, failures, repair, restart", flush=True)


def main():
    with tempfile.TemporaryDirectory(prefix="ec-kv-tests-") as tmp:
        for k, m, block in [(2, 1, 128), (6, 3, 1024)]:
            cluster = Cluster(Path(tmp) / f"compose-{k}.json", k, m, block)
            try:
                cluster.compose("up", "-d", "--no-build", "--wait")
                cluster.check()
            except Exception:
                print(cluster.compose("logs", "--tail", "30"), file=sys.stderr)
                raise
            finally:
                cluster.compose("down", "--volumes", "--timeout", "0")
                cluster.context.term()


if __name__ == "__main__":
    main()
