CXX ?= g++
.DEFAULT_GOAL := all
bin/key_index_test: tests/key_index_test.cpp src/concurrent_index.h src/stripe_barrier.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
all: bin/key_index_test
test: key-index-test
.PHONY: key-index-test
key-index-test: bin/key_index_test
	./bin/key_index_test
bin/ec_node bin/ec_proxy: src/concurrent_server.h
bin/ec_proxy: src/concurrent_index.h src/stripe_barrier.h
bin/ec_node: src/node_holes.h src/mosaic_allocator.h
bin/ec_node: src/node_transactions.inc src/transaction_journal.h
bin/ec_proxy: src/remote_holes.h src/node_holes.h
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -Wpedantic -Werror
LDLIBS = -lzmq -lisal -pthread

.PHONY: all test clean
bin/atomic_xor_test: tests/atomic_xor_test.cpp src/atomic_xor.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ -pthread
all: bin/atomic_xor_test
test: atomic-xor-test
.PHONY: atomic-xor-test
atomic-xor-test: bin/atomic_xor_test
	./bin/atomic_xor_test
bin/ec_node bin/native_store_test bin/memcached_store_test: src/atomic_xor.h
all: bin/record_view_test bin/gc_index_test bin/ec_node bin/ec_proxy bin/ec_client bin/codec_test bin/parity_workers_test bin/gc_planner_test bin/concurrency_test bin/request_workers_test bin/memcached_store_test bin/mosaic_allocator_test bin/mosaic_load_test
all: bin/length_index_test bin/metadata_test
bin/ec_node bin/memcached_store_test: src/length_index.h
bin/metadata_test: tests/metadata_test.cpp src/compact_object.h src/compact_stripes.h src/inline_fragments.h src/mosaic_allocator.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
bin/length_index_test: tests/length_index_test.cpp src/length_index.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
# Sender traffic instrumentation is also included by Store and worker tests.
bin/ec_proxy bin/metadata_test bin/mosaic_allocator_test: src/metadata_memory.h
bin/ec_node bin/ec_proxy: src/record_view.h
bin/ec_node: src/frame_scratch.h
bin/ec_node bin/ec_proxy bin/parity_workers_test bin/concurrency_test bin/request_workers_test: src/traffic.h
bin:
	mkdir -p bin
bin/ec_node: src/node.cpp src/node_batch.inc src/profile.h src/common.h src/codec.h src/parity_workers.h src/memcached_store.h src/serial_memcached_store.h src/chunked_memcached_store.h src/memcached_io.h src/memcached_client.h | bin
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS) -lmemcached
bin/ec_proxy: src/proxy.cpp src/profile.h src/codec.h src/common.h src/parity_workers.h src/request_workers.h src/gc_planner.h src/gc_index.h src/gc_store.inc src/mosaic_store.h src/mosaic_allocator.h src/compact_object.h src/compact_stripes.h src/mosaic_load.h | bin
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)
bin/ec_client: src/client.cpp src/common.h | bin
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDLIBS)
bin/codec_test: tests/codec_test.cpp src/codec.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
bin/parity_workers_test: tests/parity_workers_test.cpp src/parity_workers.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
bin/record_view_test: tests/record_view_test.cpp src/record_view.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
bin/gc_index_test: tests/gc_index_test.cpp src/gc_index.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
bin/gc_planner_test: tests/gc_planner_test.cpp src/gc_planner.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
bin/mosaic_allocator_test: tests/mosaic_allocator_test.cpp src/mosaic_allocator.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
bin/mosaic_load_test: tests/mosaic_load_test.cpp src/mosaic_load.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
bin/concurrency_test: tests/concurrency_test.cpp src/parity_workers.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
bin/request_workers_test: tests/request_workers_test.cpp src/request_workers.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
bin/memcached_store_test: tests/memcached_store_test.cpp src/memcached_store.h src/serial_memcached_store.h src/chunked_memcached_store.h src/memcached_io.h src/memcached_client.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS) -lmemcached
test: all
	./bin/metadata_test
	./bin/length_index_test
	./bin/record_view_test
	./bin/codec_test
	./bin/parity_workers_test
	./bin/gc_planner_test
	./bin/gc_index_test
	./bin/concurrency_test
	./bin/request_workers_test
	./bin/mosaic_allocator_test
	./bin/mosaic_load_test
clean:
	rm -rf bin

bin/ec_node bin/memcached_store_test: src/native_store.h src/native_io.h
bin/ec_node bin/memcached_store_test bin/native_store_test: src/partial_ranges.h
bin/native_store_test: tests/native_store_test.cpp src/native_store.h src/native_io.h src/frame_scratch.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
all: bin/native_store_test
test: native-test
.PHONY: native-test
native-test: bin/native_store_test
	./bin/native_store_test
	./bin/native_io_test

bin/native_io_test: tests/native_io_test.cpp src/native_io.h src/common.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@ $(LDLIBS)
native-test: bin/native_io_test

bin/ec_proxy: src/compact_baseline.h
bin/compact_baseline_test: tests/compact_baseline_test.cpp src/compact_baseline.h | bin
	$(CXX) $(CXXFLAGS) -Isrc $< -o $@
all: bin/compact_baseline_test
test: compact-baseline-test
.PHONY: compact-baseline-test
compact-baseline-test: bin/compact_baseline_test
	./bin/compact_baseline_test

bin/metadata_test: src/compact_baseline.h
