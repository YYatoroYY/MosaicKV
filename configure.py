#!/usr/bin/env python3
"""Generate a Compose cluster with exactly one storage node per container."""
import argparse
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent


def generate(k, m, block_size, nodes, name, port, output, strategy="mosaic", coding_unit="object", gc_options=None,
             alignment_chunk=4096, fmax=4, tau=8, workload_phase="load", storage_backend=None, mosaic_append=None,
             mosaic_space_fallback=None):
    if mosaic_space_fallback not in (None, "none", "family_load"):
        raise ValueError("invalid Mosaic space fallback policy")
    if mosaic_append not in (None, False, True):
        raise ValueError("invalid Mosaic append switch")
    if storage_backend not in (None, "memcached", "native"):
        raise ValueError("invalid storage backend")
    if workload_phase not in ("load", "resize"):
        raise ValueError("invalid workload phase")
    if coding_unit not in ("object", "block"):
        raise ValueError("invalid coding unit")
    if strategy not in ("rmw", "foreground_regroup", "fastgc", "mosaic", "inplace"):
        raise ValueError("invalid update strategy")
    if strategy in ("fastgc", "inplace") and coding_unit != "object":
        raise ValueError("strategy requires object coding units")
    if not (isinstance(alignment_chunk, int) and 64 <= alignment_chunk <= 12 * 1024 * 1024 and
            isinstance(fmax, int) and 1 <= fmax <= 4096 and isinstance(tau, int) and 0 <= tau <= 1048576):
        raise ValueError("invalid Mosaic alignment chunk, Fmax or tau")
    gc = {"GC_START_PERCENT": 20, "GC_STOP_PERCENT": 10, "GC_INTERVAL_MS": 1000,
          "GC_BATCH_STRIPES": 32, "GC_NODE_CAPACITY_BYTES": 0,
          "GC_SPACE_START_PERCENT": 80, "GC_SPACE_STOP_PERCENT": 70, "GC_PAUSED": 0,
          "GC_EVERY_UPDATES": 5000 if strategy == "fastgc" else 0}
    if gc_options:
        if set(gc_options) - set(gc):
            raise ValueError("unknown GC setting")
        gc.update(gc_options)
    if any(not isinstance(value, int) for value in gc.values()):
        raise ValueError("GC settings must be integers")
    if not (0 <= gc["GC_STOP_PERCENT"] < gc["GC_START_PERCENT"] <= 100 and
            0 <= gc["GC_SPACE_STOP_PERCENT"] < gc["GC_SPACE_START_PERCENT"] <= 100 and
            10 <= gc["GC_INTERVAL_MS"] <= 3600000 and 2 <= gc["GC_BATCH_STRIPES"] <= 128 and
            0 <= gc["GC_NODE_CAPACITY_BYTES"] <= 2**60 and gc["GC_PAUSED"] in (0, 1) and
            0 <= gc["GC_EVERY_UPDATES"] <= 1000000000):
        raise ValueError("invalid GC thresholds or limits")
    if not (k >= 1 and m >= 1 and k + m <= 32):
        raise ValueError("require K >= 1, M >= 1, K+M <= 32")
    if not (k + m <= nodes <= 256):
        raise ValueError("require K+M <= node count <= 256")
    if not (64 <= block_size <= 1024 * 1024):
        raise ValueError("block size must be 64..1048576 bytes")
    if not (1 <= port <= 65535) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name):
        raise ValueError("invalid port or Compose project name")
    output = Path(output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    config = output.with_suffix(".nodes.conf")
    config.write_text("".join(f"{i} node{i} 7000\n" for i in range(nodes)))
    image = "${EC_IMAGE:-mosaickv:local}"
    services = {}
    for i in range(nodes):
        services[f"node{i}"] = {
            "image": image, "command": ["ec_node_service"], "restart": "unless-stopped", "read_only": True,
            "environment": {"NODE_ID": str(i), "EC_K": str(k), "EC_M": str(m),
                            "BLOCK_SIZE": str(block_size), "NODES_FILE": "/app/nodes.conf",
                            "NODE_TIMEOUT_MS": "${NODE_TIMEOUT_MS:-1000}",
                            "MEMCACHED_MEMORY_MB": "${MEMCACHED_MEMORY_MB:-512}",
                            "MEMCACHED_MAX_ITEM_MB": "${MEMCACHED_MAX_ITEM_MB:-128}",
                            "MEMCACHED_THREADS": "${MEMCACHED_THREADS:-4}",
                            "MOSAIC_PARALLEL_IO": "0",
                            "MOSAIC_BATCH_IO": "1" if strategy == "mosaic" else "0",
                            "MOSAIC_IO_THREADS": "${MOSAIC_IO_THREADS:-4}",
                            "MOSAIC_CHUNK_BYTES": "${MOSAIC_CHUNK_BYTES:-262144}",
                            "MEMCACHED_PORT": "11211", "MEMCACHED_TIMEOUT_MS": "${MEMCACHED_TIMEOUT_MS:-1000}"},
            "volumes": [f"{config}:/app/nodes.conf:ro"],
        }
    services["proxy"] = {
        "image": image, "build": {"context": str(ROOT), "target": "runtime"},
        "command": ["ec_proxy"], "restart": "unless-stopped", "read_only": True,
        "environment": {"EC_K": str(k), "EC_M": str(m), "BLOCK_SIZE": str(block_size),
                        "NODES_FILE": "/app/nodes.conf", "UPDATE_STRATEGY": strategy, "CODING_UNIT": coding_unit,
                        "NODE_TIMEOUT_MS": "${NODE_TIMEOUT_MS:-1000}",
                        "STATS_TIMEOUT_MS": "${STATS_TIMEOUT_MS:-10000}"},
        "ports": [f"127.0.0.1:{port}:5555"],
        "volumes": [f"{config}:/app/nodes.conf:ro"],
        "depends_on": [f"node{i}" for i in range(nodes)],
        "healthcheck": {"test": ["CMD", "ec_client", "--endpoint", "tcp://127.0.0.1:5555", "ping"],
                        "interval": "5s", "timeout": "3s", "retries": 10},
    }
    services["proxy"]["environment"].update({key: str(int(value)) for key, value in gc.items()})
    if strategy == "mosaic":
        services["proxy"]["environment"].update({"ALIGNMENT_CHUNK": str(alignment_chunk),
                                               "MOSAIC_FMAX": str(fmax), "MOSAIC_TAU": str(tau),
                                               "MOSAIC_PLACEMENT": "${MOSAIC_PLACEMENT:-parity_bandwidth}",
                                               "MOSAIC_SPACE_FALLBACK": mosaic_space_fallback or "${MOSAIC_SPACE_FALLBACK:-family_load}",
                                               "MOSAIC_APPEND": "${MOSAIC_APPEND:-0}" if mosaic_append is None else str(int(mosaic_append)),
                                               "MOSAIC_REBALANCE_EVERY": "${MOSAIC_REBALANCE_EVERY:-1}"})
    for service in services.values():
        service["environment"]["STORAGE_BACKEND"] = storage_backend or "${STORAGE_BACKEND:-native}"
        service["environment"]["NATIVE_MEMORY_MB"] = "${NATIVE_MEMORY_MB:-512}"
        service["environment"]["EC_TRAFFIC"] = "${EC_TRAFFIC:-1}"
        service["environment"]["EC_WORKLOAD_PHASE"] = workload_phase
    output.write_text(json.dumps({"name": name, "services": services}, indent=2) + "\n")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-backend", choices=["memcached", "native"], default=None,
                        help="one backend for all services; otherwise STORAGE_BACKEND env, default native")
    parser.add_argument("--k", type=int, default=6)
    parser.add_argument("--m", type=int, default=3)
    parser.add_argument("--block-size", type=int, default=4096, help="legacy packing size; object stripes use their longest object")
    parser.add_argument("--coding-unit", choices=["object", "block"], default="object",
                        help="object layout (default); block retains legacy experiments")
    parser.add_argument("--strategy", choices=["rmw", "foreground_regroup", "fastgc", "mosaic", "inplace"], default="mosaic",
                        help="KVMD (baseline 1): foreground_regroup; fastgc (baseline 2): fastgc; InPlace: inplace; rmw: fixed-length only")
    parser.add_argument("--alignment-chunk", type=int, default=4096, help="Mosaic EC chunk bytes")
    parser.add_argument("--fmax", type=int, default=4, help="Mosaic maximum fragments per value")
    parser.add_argument("--tau", type=int, default=8, help="Mosaic small-tail preference in bytes")
    parser.add_argument("--mosaic-append", action="store_true", default=None,
                        help="retain existing fragments on APPEND; otherwise MOSAIC_APPEND env, default off")
    parser.add_argument("--mosaic-space-fallback", choices=["none", "family_load"],
                        help="Mosaic full-update fallback; otherwise MOSAIC_SPACE_FALLBACK env, default family_load")
    parser.add_argument("--workload-phase", choices=["load", "resize"], default="load",
                        help="start with unmeasured load (default); switch using ec_client phase resize")
    parser.add_argument("--gc-start-percent", type=int, default=20)
    parser.add_argument("--gc-stop-percent", type=int, default=10)
    parser.add_argument("--gc-interval-ms", type=int, default=1000)
    parser.add_argument("--gc-batch-stripes", type=int, default=32)
    parser.add_argument("--gc-node-capacity-bytes", type=int, default=0, help="per-node managed-space watermark; 0 disables space trigger")
    parser.add_argument("--gc-space-start-percent", type=int, default=80)
    parser.add_argument("--gc-space-stop-percent", type=int, default=70)
    parser.add_argument("--gc-paused", action="store_true", help="start with automatic compaction paused; GC RUN still works")
    parser.add_argument("--gc-every-updates", type=int, default=None,
                        help="background drain every N committed UPDATEs (fastgc default: 5000); 0 uses threshold scheduling")
    parser.add_argument("--nodes", type=int, help="defaults to K+M")
    parser.add_argument("--name", default="mosaickv")
    parser.add_argument("--port", type=int, default=15555)
    parser.add_argument("--output", type=Path, default=ROOT / "compose.generated.json")
    args = parser.parse_args()
    try:
        print(generate(args.k, args.m, args.block_size, args.nodes if args.nodes is not None else args.k + args.m,
                       args.name, args.port, args.output, args.strategy, args.coding_unit,
                       {key.upper(): int(value) for key, value in vars(args).items() if key.startswith("gc_") and value is not None},
                       args.alignment_chunk, args.fmax, args.tau, args.workload_phase, args.storage_backend, args.mosaic_append,
                       args.mosaic_space_fallback))
    except ValueError as exc:
        parser.error(str(exc))
