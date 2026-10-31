# MosaicKV: Taming Size-Changing Updates in Erasure-Coded KV Stores via Split Relocation

MosaicKV is an in-memory erasure-coded key-value store for size-changing updates. Split relocation places each value in a bounded number of fragments and reuses free space on memory nodes. The proxy manages placement and object metadata. Memory nodes handle allocation, data access and parity updates using Intel ISA-L.

The interface supports PUT, GET, UPDATE, APPEND, DELETE and repair. The repository also includes KVMD, FastGC and In-Place for comparison. See [implementation notes](IMPLEMENTATION_NOTES.md) for the storage layout, update protocol and concurrency design.

## Repository layout

| Directory | Contents |
| --- | --- |
| `src/` | Client, proxy, memory nodes and erasure coding |
| `scripts/` | Local and physical-cluster startup |
| `experiments/` | Workload preparation, experiment runners and plots |
| `benchmarks/` | Replay and measurement utilities |
| `tests/` | Unit, protocol and concurrency tests |

## Build

Use Linux, a C++17 compiler, Boost, ZeroMQ and Intel ISA-L. On Ubuntu:

```bash
sudo apt-get update
sudo apt-get install g++ make libboost-dev libzmq3-dev libisal-dev libmemcached-dev
make -j2 test
```

The build produces `bin/ec_proxy`, `bin/ec_node` and `bin/ec_client`. The test target runs 16 C++ tests. Python tests use the dependencies in `experiments/requirements.txt` and run with `python3 -m unittest discover -s tests -p 'test_*.py' -v`.

For Docker Engine with Compose v2:

```bash
docker build -t mosaickv:local .
```

The image build also runs the C++ tests.

## Local quick start

Start all components on one machine with six data shards, three parity shards and native memory storage:

```bash
bash scripts/start.sh
docker compose exec -T proxy ec_client --endpoint tcp://127.0.0.1:5555 put demo hello
docker compose exec -T proxy ec_client --endpoint tcp://127.0.0.1:5555 get demo
docker compose exec -T proxy ec_client --endpoint tcp://127.0.0.1:5555 update demo longer-value
docker compose exec -T proxy ec_client --endpoint tcp://127.0.0.1:5555 stats
docker compose down
```

Clients on the host connect to `tcp://127.0.0.1:15555`. Data lives in RAM and is lost when the services stop. Configuration examples and parameter defaults are in the [experiment guide](experiments/README.md#configuration).

## Reproduce the experiments

All scripts below are in `experiments/`. The [experiment guide](experiments/README.md) contains complete commands, workload preparation and output formats. Use a fresh cluster and a new result directory for each case.

| Paper | Script | Workloads |
| --- | --- | --- |
| Exp. 1 Space amplification | `run_endpoint.py --space-sample-every` | Twitter, Random, ShareGPT |
| Exp. 2 Traffic, Exp. 3 Update latency | `run_endpoint.py --experiment update` | Twitter, Random, ShareGPT |
| Exp. 4 Read latency | `run_read.sh` | Twitter, Random, ShareGPT |
| Exp. 5 Repair | `run_repair.sh` | Twitter, Random, ShareGPT |
| Exp. 6 Load balance | `run_endpoint.py --experiment update` | Twitter, Random |
| Exp. 7 CPU | `run_endpoint.py --cpu-hosts` | Twitter, Random, ShareGPT |
| Exp. 8 Metadata and fragments | `run_endpoint.py --record-metadata-memory` | Twitter, Random, ShareGPT |
| Exp. 9 Equal-size updates | `run_fixed.sh`, `run_read.sh`, `run_repair.sh` | fixed256 |

The coding configurations are RS(6,3), RS(6,4), RS(10,4) and RS(10,6). Twitter and Random use 1, 4 and 16 KiB chunks. ShareGPT uses 400, 600 and 800 KiB chunks. Runs use Fmax=4, τ=8 and three repeats. Collect space curves separately from latency and CPU measurements.

## Physical-cluster deployment

The evaluation cluster has 18 physical nodes connected by 10 Gbps Ethernet: one client, one proxy and 16 memory nodes. Each node has two Intel Xeon Gold 5218R processors at 2.10 GHz, 80 logical threads and 188 GB of DDR4 memory. The hosts run Ubuntu 20.04.6 LTS with Linux 5.15.167.

Build or distribute the same image on each service host. Copy `nodes.conf` to every host with ordered `node_id host port` rows for IDs 0–15. Set the service configuration:

```bash
export EC_K=6 EC_M=3 UPDATE_STRATEGY=mosaic NATIVE_MEMORY_MB=8192
export ALIGNMENT_CHUNK=4096 MOSAIC_FMAX=4 MOSAIC_TAU=8
export EC_IMAGE=mosaickv:local NODES_FILE="$PWD/nodes.conf"
```

Start each memory node with its own ID, then start the proxy:

```bash
# Memory host 0; use that host's ID on each other memory host.
bash scripts/start_physical.sh node 0
# Proxy host, after all memory services are ready.
bash scripts/start_physical.sh proxy
```

Allow port 7000 between services and port 5555 from the client to the proxy. Run experiments from the client with `--endpoint tcp://PROXY_IP:5555`. The [deployment instructions](experiments/README.md#physical-cluster-setup) include a complete node list and environment capture.

## License

MosaicKV uses the [MIT License](LICENSE). Dependencies retain their own licenses.
