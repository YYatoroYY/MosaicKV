# Implementation notes

MosaicKV consists of a client library, a proxy and memory nodes. Reed–Solomon coding uses Intel ISA-L. Data, object metadata and transaction journals live in RAM.

## Layout and placement

Memory nodes maintain one hole index per coding position. Each index uses red-black trees ordered by length and address. `NodeHoles` executes allocation and release under a mutex. Replies carry the capacity of the largest Fmax holes, which the proxy caches for placement.

Placement ranks stripe rotations by the combined data and parity write load on their parity nodes. Within a rotation, it selects the least-loaded data node with sufficient space. Equal-size updates keep their existing fragments.

A new stripe materializes its selected data chunk and parity chunks. Other data positions remain virtual zeroes. Allocation reuses an available position in an existing stripe before creating another stripe. Release coalesces adjacent holes. Reused ranges retain their old bytes until the next update computes a delta against them.

## Updates and recovery

The data node saves the overwritten ranges and computes parity deltas. Parity nodes apply those deltas using atomic XOR. Once data and parity writes complete, the proxy publishes the new fragment map. Commit releases the saved transaction state.

If the update aborts, the data node restores the old ranges. Parity nodes undo applied deltas with a second XOR. Monotonic transaction IDs identify duplicate requests. An abort record also rejects a delayed application of the same delta. This rollback protocol handles request failures while service processes are running.

Degraded reads and repair wait for updates to the affected stripes to settle. A stripe counter tracks active transactions, including updates awaiting a confirmed outcome.

## Concurrency

The proxy stores objects in a sharded concurrent hash index. Each entry publishes an immutable fragment map through an atomic shared pointer. Writers serialize per key. An in-place update takes that key's exclusive reader lock. Different keys can execute transaction I/O concurrently.

GET holds a reference to its map until the read finishes. Relocation and deletion publish immediately, then defer release of old fragments until those references expire. Retained APPEND operations share the same reader epoch, so an earlier reader can finish after a later relocation.

Lock order is key writer, key reader gate, proxy metadata, then worker queue. Network waits release the metadata lock. Memory nodes use separate pools for transaction coordination and parity requests. Coordinators release the service mutex while waiting for remote replies. Proxy connections also reserve separate read and control lanes.

`STATS` reports concurrent operations, pending keys and bytes retained by readers. Those bytes remain allocated until reclamation completes.

| Code | Role |
| --- | --- |
| `src/mosaic_store.h` | Placement and object operations |
| `src/node_holes.h` | Allocation and release |
| `src/node_transactions.inc` | Commit and rollback |
| `src/concurrent_index.h` | Hash index and version references |
| `src/stripe_barrier.h` | Coordination with degraded reads |

## Tests

`make test` runs 16 C++ tests. CI also runs address and undefined-behavior checks.

Protocol tests run real node processes and cover retry, rollback and degraded reads:

```bash
python3 -m unittest discover -s tests -p test_mosaic_protocol.py -v
```

Concurrency tests delay reader replies to check publication and reclamation:

```bash
python3 -m unittest discover -s tests -p test_key_concurrency.py -v
```

Measurement tests check CPU accounting and space samples:

```bash
python3 -m unittest discover -s tests -p test_measurement_tools.py -v
```
