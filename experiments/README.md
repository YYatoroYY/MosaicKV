# Running MosaicKV experiments

Run commands from the repository root. Local runners create Docker clusters on one host. Endpoint runners connect to services on the physical cluster described in the [README](../README.md).

## Setup

Use Python 3.9+ and a JDK for YCSB generation. Local orchestration also uses Linux, Docker Engine, Compose v2 and cgroup v2.

```bash
sudo apt-get install python3-venv openjdk-17-jdk-headless
python3 -m venv .venv
. .venv/bin/activate
pip install -r experiments/requirements.txt
docker build -t mosaickv:local .
```

Start each case with empty services and `EC_WORKLOAD_PHASE=load`. Use a new result directory for each run. Keep the workload, coding parameters and capacity the same across schemes, and run each case three times.

## Configuration

Generate a local configuration with:

```bash
python3 configure.py --strategy mosaic --storage-backend native \
  --k 6 --m 3 --alignment-chunk 4096 --fmax 4 --tau 8 \
  --name mosaickv-demo --port 15565 --output compose.demo.json
docker compose -f compose.demo.json up -d --build --wait
docker compose -f compose.demo.json down
```

| System | `--strategy` or `--schemes` value |
| --- | --- |
| MosaicKV | `mosaic` |
| KVMD | `foreground_regroup` |
| FastGC | `fastgc` |
| In-Place | `inplace` |

| Parameter | Default | Setting |
| --- | --- | --- |
| Data and parity shards | k=6, m=3 | `--k`, `--m`, `EC_K`, `EC_M` |
| Alignment chunk | 4096 bytes | `--alignment-chunk`, `ALIGNMENT_CHUNK` |
| Fragment limit | Fmax=4 | `--fmax`, `MOSAIC_FMAX` |
| Small-tail preference | τ=8 bytes | `--tau`, `MOSAIC_TAU` |
| Storage backend | native | `--storage-backend`, `STORAGE_BACKEND` |
| Native payload capacity | 512 MiB per node | `NATIVE_MEMORY_MB` |
| FastGC trigger | 5000 committed updates | `--gc-every-updates`, `GC_EVERY_UPDATES` |

MosaicKV uses native memory. The comparison implementations also support memcached. `MOSAIC_APPEND=1` enables fragment-preserving append. Backend I/O settings are `MOSAIC_BATCH_IO`, `MOSAIC_PARALLEL_IO`, `MOSAIC_IO_THREADS` and `MOSAIC_CHUNK_BYTES`. Generated configurations enable batch I/O for MosaicKV. See `configure.py --help` for all options.

## Physical-cluster setup

Build the same image on the proxy and 16 memory hosts, or distribute it with `docker save` and `docker load`. Use the same `nodes.conf` on every host. Replace these addresses with your memory-node addresses:

```text
0 10.0.0.10 7000
1 10.0.0.11 7000
2 10.0.0.12 7000
3 10.0.0.13 7000
4 10.0.0.14 7000
5 10.0.0.15 7000
6 10.0.0.16 7000
7 10.0.0.17 7000
8 10.0.0.18 7000
9 10.0.0.19 7000
10 10.0.0.20 7000
11 10.0.0.21 7000
12 10.0.0.22 7000
13 10.0.0.23 7000
14 10.0.0.24 7000
15 10.0.0.25 7000
```

Follow the service startup commands in the [README](../README.md#physical-cluster-deployment). Set `UPDATE_STRATEGY` consistently on all hosts. After saving a case, remove `mosaickv-node` from each memory host and `mosaickv-proxy` from the proxy host with `docker rm -f`. Start fresh services for the next case.

Save each host's configuration with its results:

```bash
python3 experiments/capture_environment.py --output host-environment.json
```

Record the host role and network configuration alongside this file.

## Prepare Twitter and Random

The exporter uses YCSB 0.17.0. These commands generate 3 million initial keys and 30 million updates with Zipfian key selection:

```bash
python3 experiments/generate_ycsb.py --workload twitter \
  --records 3000000 --operations 30000000 --key-bytes 32 --initial-bytes 64 \
  --distribution zipfian --output workloads/twitter
python3 experiments/generate_ycsb.py --workload random \
  --records 3000000 --operations 30000000 --key-bytes 128 --initial-bytes 256 \
  --distribution zipfian --output workloads/random
```

For a small check, use `--records 120 --operations 240`. Use `--distribution uniform` for uniform key selection. The output includes `workload.json`, `load.tsv`, `run.tsv` and `summary.json`. Reuse these files across comparisons. YCSB dependencies download on first use, or `--ycsb-home` selects an existing installation.

Twitter lengths follow the reference distribution for Twemcache clusters 12, 15, 31 and 37 from [Yang et al., OSDI 2020](https://www.usenix.org/conference/osdi20/presentation/yang). The histogram and source details are in [twitter_sizes.csv](data/twitter_sizes.csv) and [twitter_sizes.source.json](data/twitter_sizes.source.json). YCSB supplies the keys and request order. Lengths range from 1 to 4096 B, with 87.1% at most 128 B and 12.0% between 513 B and 4 KiB. Within 1–32 B, lengths are uniform. An alternative `--twitter-histogram` accepts `new_size_bytes,requests` rows for sizes 1–4096.

Random samples lengths uniformly from 1–4096 B. Resize generation replaces equal-size candidates until it reaches the requested operation count.

## Prepare ShareGPT

The workload uses both HTML-cleaned files from `anon8231489123/ShareGPT_Vicuna_unfiltered`. [sharegpt.source.json](data/sharegpt.source.json) records the revision and hashes. After deduplication, the paper configuration contains 72,964 sessions and approximately 982,000 append updates.

```bash
python3 experiments/prepare_sharegpt.py --download \
  --source workloads/sharegpt-source --output workloads/sharegpt
```

Each session is one key. Its first message initializes the value, and subsequent messages append UTF-8 JSONL records. A seeded schedule interleaves sessions while preserving their message order. The replay bundle contains `messages.bin`, `sessions.jsonl`, `dataset/initial_kv.jsonl` and `testset/schedule.bin`. The manifest records exact counts and hashes.

Use the default `--update-mode full` for comparisons: every scheme receives the complete updated value. `--update-mode append` sends only the next message through MosaicKV's APPEND interface. Both modes replay the same conversations.

## Local experiments

The local runners create isolated Docker clusters. The default comparison uses native memory, k=6, m=3, 4096 B chunks, Fmax=4, τ=8 and three repeats.

```bash
bash experiments/run_update.sh --workload-file workloads/twitter/workload.json \
  --schemes mosaic foreground_regroup fastgc inplace --configs 6+3 \
  --alignment-chunk 4096 --fmax 4 --tau 8 --native-memory-mb 8192 \
  --repeats 3 --output results/twitter-update
bash experiments/run_space.sh --workload-file workloads/random/workload.json \
  --native-memory-mb 8192 --repeats 3 --output results/random-space
bash experiments/run_metadata.sh --workload-file workloads/twitter/workload.json \
  --native-memory-mb 8192 --repeats 3 --output results/twitter-metadata
bash experiments/run_alignment.sh --workload-file workloads/twitter/workload.json \
  --alignment-chunks 1024 4096 16384 --native-memory-mb 8192 \
  --repeats 3 --output results/twitter-alignment
```

Use `--configs 6+3 6+4 10+4 10+6` for the coding sweep. `run_space.sh` samples every 1000 completed updates. `--space-sample-every N` sets the interval, and `0` selects boundary snapshots. `run_metadata.sh` adds catalog snapshots. Workload and payload arrays are held in client memory, so start with a small case before a full replay.

## Physical-cluster updates

On the client host, replace `PROXY_IP` with the proxy address:

```bash
python3 experiments/run_endpoint.py --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/twitter/workload.json --experiment update \
  --record-metadata-memory --output results/physical-twitter
```

For ShareGPT, start the proxy with `ALIGNMENT_CHUNK=614400`:

```bash
bash experiments/run_sharegpt.sh --endpoint tcp://PROXY_IP:5555 \
  --sharegpt-bundle workloads/sharegpt --update-mode full \
  --record-metadata-memory --output results/physical-sharegpt-600kib
```

For the ShareGPT chunk sweep, restart the cluster with `ALIGNMENT_CHUNK=409600`, `614400` and `819200` (400, 600 and 800 KiB). For Twitter and Random, use 1024, 4096 and 16384 B. Give each case a distinct result directory.

## Physical-cluster CPU time (Exp. 7)

The client reads service CPU counters through SSH on the proxy host and every memory host. Each host needs Python 3, Docker inspection permission and readable cgroups. The collector supports cgroup v2 and Ubuntu 20.04 cgroup v1. Configure SSH keys and host aliases for the client account.

Create `cpu-hosts.json` from `nodes.conf`:

```bash
python3 - <<'PY'
import json
from pathlib import Path
services = {'proxy': {'host': 'bench@PROXY_IP', 'container': 'mosaickv-proxy'}}
for line in Path('nodes.conf').read_text().splitlines():
    fields = line.split('#', 1)[0].split()
    if not fields:
        continue
    node_id, host, port = fields
    services['node' + node_id] = {'host': 'bench@' + host, 'container': 'mosaickv-node'}
Path('cpu-hosts.json').write_text(json.dumps({'services': services}, indent=2) + '\n')
PY
```

Replace `bench@PROXY_IP` and the SSH accounts with your settings. Include the proxy and every node ID returned by `STATS`, including spare nodes. The collector checks service identities and counter availability. `--cpu-timeout` sets the per-host timeout, with a default of 30 seconds.

Run update and read measurements on separate fresh clusters with matching configurations:

```bash
python3 experiments/run_endpoint.py --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/twitter/workload.json --experiment update \
  --cpu-hosts cpu-hosts.json --output results/physical-twitter-cpu-update
# Restart an empty cluster before the read case.
python3 experiments/run_endpoint.py --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/twitter/workload.json --experiment read --reads 100000 \
  --cpu-hosts cpu-hosts.json --output results/physical-twitter-cpu-read
```

For ShareGPT, substitute `--sharegpt-bundle workloads/sharegpt`. `cpu-accounting.json` saves counter snapshots, per-service deltas and collection windows. CPU seconds sum across cores. `metrics.json` reports `cpu_proxy_seconds`, `cpu_storage_seconds`, `cpu_total_seconds` and user and system components. `cpu_ms_per_operation` divides total service CPU by the successful operation count. The corresponding fields are `cpu_ms_per_update`, `cpu_ms_per_read` and `cpu_ms_per_repair`.

Update totals include FastGC maintenance through `QUIESCE`. Foreground and maintenance CPU are also reported separately. Read sampling starts after workload preparation and query selection. It ends before validation and reporting. Host counters are collected in parallel with local timestamps. The totals include service activity during collection and client gaps. Cgroup v1 components use scheduler-tick resolution.

## Space amplification during updates (Exp. 1)

Collect a separate replay for the curve:

```bash
python3 experiments/run_endpoint.py --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/twitter/workload.json --experiment update \
  --space-sample-every 10000 --output results/physical-twitter-space
python3 experiments/plot_space_trace.py --input results/physical-twitter-space \
  --output results/physical-twitter-space
```

`--space-sample-every N` captures committed space before updates, every N completed updates and at the end. FastGC adds a `post_gc` sample after `QUIESCE`. `space-trace.csv` records update progress, elapsed time, live bytes, payload and amplification. `space-trace-stats.jsonl` stores the original snapshots.

Plot multiple result directories with `--input DIR1 DIR2 ...`. The script writes PNG and PDF curves against update progress. Repeats appear separately, and hollow markers show post-GC values. Local `run_space.sh` produces the same plots.

Space sampling slows down the replay, so collect latency and CPU in separate runs without `--space-sample-every`.

## Read latency

The runner prepares the workload, completes FastGC maintenance and issues 100,000 Zipfian GETs. It times each request and checks the returned value afterwards.

```bash
bash experiments/run_read.sh --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/twitter/workload.json \
  --reads 100000 --zipf-theta 0.99 --output results/physical-twitter-read
```

For ShareGPT, replace `--workload-file ...` with `--sharegpt-bundle workloads/sharegpt`. The manifest records the seed and Zipf exponent.

## Repair latency

The repair runner prepares the workload and measures data reconstruction and writeback:

```bash
bash experiments/run_repair.sh --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/twitter/workload.json \
  --repair-count 1000 --output results/physical-twitter-repair
```

Use `--sharegpt-bundle workloads/sharegpt` for conversations. `REPAIR_BENCH` reconstructs an object from surviving helpers and writes it to its reachable data node. The timer covers reconstruction and writeback. In-Place repair preserves the encoded suffix retained after a shrink.

## Equal-size updates

Generate the fixed256 workload and replay replacements of the same length:

```bash
python3 experiments/generate_ycsb.py --workload random --update-kind fixed \
  --records 3000000 --operations 30000000 --key-bytes 128 --initial-bytes 256 \
  --distribution zipfian --output workloads/fixed256
bash experiments/run_fixed.sh --endpoint tcp://PROXY_IP:5555 \
  --workload-file workloads/fixed256/workload.json --output results/physical-fixed256
```

Use the read and repair commands above with `workloads/fixed256/workload.json` for the other fixed-size cases.

## Results and metrics

| Result | Files |
| --- | --- |
| Latency and service rate | `summary.csv`, `metrics.json`, `latency.csv` |
| Traffic and load balance | `per-node.csv`; local runs also write `per-link.csv` |
| CPU | `cpu-accounting.json` |
| Endpoint boundaries | `stats-before.json`, `stats-after.json` |
| Local space summaries | `space-summary.csv`, `space-per-run.csv` |
| Space curves | `space-trace.csv`, `space-trace-stats.jsonl`, `space-amplification-progress.png` and `.pdf` |
| FastGC peaks | `space-peaks.csv`, `space-amplification-peaks.csv` |
| Metadata | `metadata-memory.json`; local runs also write `metadata-summary.csv` |
| Fragments | Endpoint `mean_fragments`; local `fragments-summary.csv` and `validation.json` |

`--record-metadata-memory` enables endpoint metadata snapshots. Chunk sweeps use separate `chunk-1024`, `chunk-4096` and `chunk-16384` directories. A successful run records `passed: true` in `manifest.json`. Keep the workload, host configuration and results together.

| Metric | Definition |
| --- | --- |
| Latency | Time for the client request and response |
| Local throughput | Closed-loop sequential updates per second |
| Endpoint service rate | Operations divided by summed RPC duration |
| Traffic | Application-frame bytes: storage RX+TX plus proxy backend TX |
| Load imbalance | λ = (Tmax − Tavg) / Tavg, where T is cumulative RX+TX per memory node |
| Space amplification | Active payload / ((1 + m/k) × logical live bytes) |
| Fragments | Final extent count divided by real workload keys |
| Metadata | Catalog fields, heap allocations and indexes at the proxy and memory nodes |

Space includes materialized data and parity chunks, freed chunks awaiting reuse, and fragments retained by active readers. Virtual zeroes consume no chunk payload. FastGC final counters follow `QUIESCE`. Traffic uses application frames, and fragment counts use real workload keys. CV and the other load statistics are saved with λ.

## Checks

Generator and measurement checks:

```bash
python3 experiments/test_generate_ycsb.py --output /tmp/mosaickv-ycsb-check
python3 -m unittest discover -s tests -p test_measurement_tools.py -v
EC_IMAGE=mosaickv:local python3 tests/endpoint_smoke.py
```

The endpoint smoke test uses Docker services to check CPU collection, space tracing and workload replay. Range-read and repair checks:

```bash
EC_IMAGE=mosaickv:local python3 tests/single_kv_read.py
for scheme in foreground_regroup fastgc inplace; do
  EC_IMAGE=mosaickv:local python3 tests/baseline_range_repair.py \
    --strategy "$scheme" --backend native --output "/tmp/${scheme}-range-repair.json"
done
```

Use `--backend memcached` for the alternative backend. Protocol and concurrency test commands are in [implementation notes](../IMPLEMENTATION_NOTES.md#tests).

YCSB references: [CoreWorkload](https://github.com/brianfrankcooper/YCSB/blob/0.17.0/core/src/main/java/site/ycsb/workloads/CoreWorkload.java), [DB binding interface](https://github.com/brianfrankcooper/YCSB/blob/0.17.0/core/src/main/java/site/ycsb/DB.java).
