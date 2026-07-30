---
title: Benchmark
parent: State Backend
nav_order: 10
---

# Benchmark

This page compares Cobble and RocksDB as Flink state backends using Nexmark for
end-to-end streaming throughput, the Flink state micro-benchmark for isolated
state operations, and a remote checkpoint rescale benchmark.

All results were produced from a single local machine (Apple Silicon, macOS,
32 GiB RAM, 10 cores) with the same Cobble and RocksDB configuration on both
sides. The comparison is directional; absolute numbers depend on hardware and
workload.

## Test setup

| Item | Value |
|---|---|
| Cobble version | 0.3.0-1-flink-1.17 |
| Flink | 1.17.2 |
| Java | OpenJDK 11.0.29 |

Both backends use Flink managed memory exclusively (no fixed-per-slot memory),
so each backend receives the same memory budget in every comparison. No
profiling, tuning, or diagnostic options were enabled during the runs. The
Nexmark and state micro-benchmark suites were each run in isolation with no
other workload on the machine.

## Nexmark streaming benchmark

[Nexmark](https://github.com/nexmark/nexmark) is a streaming benchmark that
generates a synthetic stream of auction events (persons, auctions, bids) and
runs SQL queries against it. We use it to measure end-to-end pipeline
throughput under realistic stateful workloads.

### Configuration

| Item | Value |
|---|---|
| Events per run | 15,000,000 |
| Source rate ceiling | 10,000,000 events/s |
| Parallelism | 1 |
| TaskManager slots | 1 |
| Checkpointing | disabled |
| Sink | Nexmark blackhole |
| JobManager / TaskManager memory | 8 GiB each |
| State TTL | 24 h (both backends, `table.exec.state.ttl`) |
| Generator seed | fixed per round (1001, 2002, 3003) |
| Base event time | fixed (`2024-01-01 00:00:00 UTC`) |
| Rounds | 3 (one per seed, results averaged) |
| Queries | q3, q4, q5, q7, q8, q9, q11, q12, q15, q16, q17, q18, q19, q20, q23 |

To make the comparison reproducible, the Nexmark source was patched to
accept a fixed `generator.seed` and `base.time`. Each round runs with a
different fixed seed (1001, 2002, 3003) so the three rounds exercise
distinct event streams, while every run of the same seed produces byte-
identical data. A 24 h state TTL (`table.exec.state.ttl`) is applied
identically to both backends; Cobble honors it natively via per-write
expiry. Each query/backend pair was run once per seed; the table below
reports the average duration across the three seeds.

### Results

The "Cobble throughput" column is `(RocksDB duration / Cobble duration - 1) * 100%`;
a positive value means Cobble finished faster.

| Query | RocksDB duration | Cobble duration | Cobble throughput |
|---|---:|---:|---:|
| q3 | 31.7s | 28.3s | +12.0% |
| q4 | 123.5s | 94.3s | +30.9% |
| q5 | 97.8s | 69.1s | +41.5% |
| q7 | 182.9s | 89.7s | +103.9% |
| q8 | 28.1s | 28.1s | +0.1% |
| q9 | 222.2s | 183.1s | +21.3% |
| q11 | 106.4s | 99.9s | +6.6% |
| q12 | 33.3s | 33.2s | +0.3% |
| q15 | 77.1s | 79.6s | -3.1% |
| q16 | 299.2s | 270.8s | +10.5% |
| q17 | 39.5s | 41.5s | -4.9% |
| q18 | 76.2s | 51.6s | +47.6% |
| q19 | 57.1s | 46.4s | +23.0% |
| q20 | 156.4s | 115.0s | +36.1% |
| q23 | 280.0s | 232.3s | +20.5% |
| **Total** | **1811.3s** | **1462.9s** | **+23.8%** |

### Summary

- Cobble is faster in **11 of 15** queries (above 5%) and within 5% in 4
  queries (q8, q12, q15, q17).
- The total Cobble duration is **19.2% lower** than RocksDB
  (1462.9s vs 1811.3s), a **+23.8%** throughput advantage.
- The largest Cobble lead is on **q7** (+103.9%), a stateful join with heavy
  state access.
- With the 24 h TTL and reproducible data, q8, q12, q15, and q17 are within
  ±5%.

## Flink state micro-benchmark

The `cobble-state-bench` module is a JMH in-process benchmark that measures
individual state operation throughput (ValueState, ListState, MapState) in
isolation, without a running Flink cluster. It is the Flink community's state
benchmark adapted to compare Cobble and RocksDB directly.

### Configuration

| Item | Value |
|---|---|
| JMH mode | Throughput |
| Unit | operations/ms |
| Threads | 1 |
| Warmup | 10 iterations, 1 second each |
| Measurement | 10 iterations, 1 second each |
| Forks | 3 |
| Backends | RocksDB, Cobble |
| Managed memory | 256 MiB (both backends, fraction 1.0) |
| Fixed-per-slot memory | none |
| JVM heap | not set |

Both backends explicitly enable managed memory and receive the full 256 MiB
budget. Neither backend uses fixed-per-slot memory or per-run tuning. The
score error is JMH's 99.9% confidence interval across 3 forks (30 measurement
samples per result).

### Results

The "Cobble throughput" column is `(Cobble score / RocksDB score - 1) * 100%`;
a positive value means Cobble has higher throughput.

| Benchmark | RocksDB | Cobble | Cobble throughput |
|---|---:|---:|---:|
| valueAdd | 588.898 ± 41.138 | 854.506 ± 26.699 | +45.1% |
| valueGet | 836.686 ± 15.678 | 878.697 ± 15.122 | +5.0% |
| valueUpdate | 592.690 ± 43.405 | 870.665 ± 47.619 | +46.9% |
| listAdd | 738.088 ± 48.829 | 1016.921 ± 54.038 | +37.8% |
| listAddAll | 475.551 ± 6.452 | 674.456 ± 16.305 | +41.8% |
| listAppend | 664.232 ± 22.524 | 857.058 ± 22.624 | +29.0% |
| listGet | 687.114 ± 18.834 | 799.233 ± 51.123 | +16.3% |
| listGetAndIterate | 669.452 ± 13.341 | 786.904 ± 13.498 | +17.5% |
| listUpdate | 651.474 ± 16.273 | 892.762 ± 37.243 | +37.0% |
| mapAdd | 586.160 ± 44.811 | 925.876 ± 61.661 | +58.0% |
| mapContains | 364.432 ± 4.686 | 538.012 ± 9.149 | +47.6% |
| mapEntries | 949.889 ± 26.460 | 928.594 ± 22.631 | -2.2% |
| mapGet | 374.723 ± 12.112 | 380.316 ± 7.709 | +1.5% |
| mapIsEmpty | 244.190 ± 14.766 | 230.685 ± 3.465 | -5.5% |
| mapIterator | 941.331 ± 23.333 | 939.407 ± 8.328 | -0.2% |
| mapKeys | 943.048 ± 28.935 | 976.894 ± 31.140 | +3.6% |
| mapPutAll | 114.450 ± 4.032 | 256.203 ± 5.456 | +123.9% |
| mapRemove | 609.061 ± 54.656 | 893.226 ± 36.671 | +46.7% |
| mapUpdate | 560.540 ± 46.639 | 945.696 ± 48.038 | +68.7% |
| mapValues | 942.460 ± 25.564 | 999.262 ± 7.742 | +6.0% |

### Summary

- Cobble has higher throughput in **14 of 20** benchmarks (above 5%), is within
  5% in 5 benchmarks, and is slower in **1** benchmark (mapIsEmpty).
- The largest Cobble lead is on **mapPutAll** (+123.9%).
- **5 of 20** benchmarks have overlapping 99.9% confidence intervals; in all
  of these the difference is within ±6%.

## Remote rescale benchmark

This benchmark measures Flink-native checkpoint rescaling against an
S3-compatible object store with added network latency. An unbounded WordCount
job initializes a fixed 4 GiB keyspace and then updates it without a source
rate limit. The keyspace is unchanged by parallelism.

### Configuration

| Item | Value |
|---|---|
| Cobble version | 0.3.0-1-flink-1.17 |
| Flink | 1.17.2 |
| State | 524,288 keys, 8 KiB payload per key (4 GiB configured) |
| Object store | RustFS through Toxiproxy |
| Added object-store latency | 40 ± 5 ms in each direction |
| Aggregate restore bandwidth | 1,000 Mbit/s across all connections |
| Restore mode | native checkpoint, claim |
| Parallelism | 2 to 3, then 3 to 2 |
| Resources per subtask | 1 slot, 1 CPU, 512 MiB managed memory |
| TaskManager process memory | 2 GiB per subtask |
| Source rate limit | none |
| Execution order | RocksDB and Cobble run sequentially |

Parallelism 2 uses two one-slot TaskManagers; parallelism 3 uses three. The
total resources therefore scale in the same 2:3 ratio as the job.

Restore time is measured from submission until every job subtask reports
`RUNNING` through the Flink REST lifecycle API. A job-level `RUNNING` state is
not sufficient while any subtask remains `INITIALIZING`. The aggregate
bandwidth limit is enabled immediately before each restore submission and
removed after all subtasks reach `RUNNING`.

### Results

| Backend | 2 to 3 restore | 3 to 2 restore |
|---|---:|---:|
| RocksDB | 75.214s | 84.392s |
| Cobble | 11.172s | 10.960s |

Cobble reached `RUNNING` 6.73 times faster for scale-out and 7.70 times faster
for scale-in. The traffic shaper recorded the following aggregate bytes sent
by the object-store proxy while each restore was bandwidth-limited:

| Backend | 2 to 3 proxy bytes | 3 to 2 proxy bytes |
|---|---:|---:|
| RocksDB | 8.71 GB | 9.72 GB |
| Cobble | 0.96 GB | 0.94 GB |

Each traffic-shaper record reported a `1 Gbit` rate and nonzero traffic.
Cobble restored rescaled state files by reference instead of downloading the
complete state before startup.

The first 30-second throughput sample did not advance for either backend, so it
is excluded rather than treated as a zero-throughput result. Later windows show
that both jobs resumed processing:

| Backend | 2 to 3, 30-60s | 2 to 3, 120-180s | 3 to 2, 30-60s | 3 to 2, 120-180s |
|---|---:|---:|---:|---:|
| RocksDB | 1,275,678 events/s | 1,276,797 events/s | 830,462 events/s | 828,329 events/s |
| Cobble | 1,266,066 events/s | 1,666,907 events/s | 977,647 events/s | 1,129,954 events/s |

## Observations

The Nexmark and state micro-benchmark results show Cobble delivering higher
throughput than RocksDB under the same managed-memory budget:

- **Write-heavy operations** (valueAdd, valueUpdate, mapAdd, mapUpdate,
  mapPutAll, mapRemove) show the largest Cobble advantages in the
  micro-benchmark, and the stateful Nexmark queries (q4, q5, q7, q18, q20)
  show the largest end-to-end gains.
- **Read-only or full-scan operations** (valueGet, mapGet, mapEntries,
  mapIterator, mapKeys, mapValues) are roughly equal, with Cobble slightly
  ahead and overlapping confidence intervals.
- All 15 Nexmark queries show a Cobble gain
  or are within 5%; q12 uses its normal four-boundary processing-time result
  because an additional wall-clock window adds about 10 s of scheduling tail
  latency unrelated to backend throughput.
- The remote rescale benchmark shows a separate tradeoff: restoring shared
  files by reference substantially reduces time to `RUNNING`, while a cold
  random-read workload can remain limited by remote-storage latency until its
  working set is cached or materialized locally.

These results reflect the current default configuration of both backends
without tuning. Production performance depends on workload characteristics,
state size, hardware, and Flink configuration.
