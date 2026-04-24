# cobble-state-bench

`cobble-state-bench` contains JMH benchmarks for comparing Flink keyed state performance across:

- Flink `HashMapStateBackend` (`HEAP`)
- Flink `EmbeddedRocksDBStateBackend` (`ROCKSDB`)
- `CobbleStateBackend` (`COBBLE`)

The current suite covers `ValueState`, `ListState`, and `MapState`.

## Code source

This module is adapted from Apache Flink's benchmark project, mainly the state benchmark package:

- upstream project: <https://github.com/apache/flink-benchmarks>
- upstream package: `src/main/java/org/apache/flink/state/benchmark`

The benchmark shapes and workload categories follow the upstream Flink benchmarks, while the local helper layer was rewritten for this repository so it can:

- run against Flink `1.17`
- construct `HEAP`, `ROCKSDB`, and `COBBLE` keyed state backends in one place
- keep the benchmark entrypoints aligned with the upstream `flink-benchmarks` Maven / uber-jar workflow

## Build

From the repository root:

```bash
./mvnw verify
```

## Run with Maven

This module supports the same style as `flink-benchmarks`: build the uber jar and run it in one Maven command.

Run the full Cobble state benchmark suite from the repository root:

```bash
./mvnw -pl cobble-state-bench -am clean package exec:exec
```

Run a narrower subset by overriding the benchmark regex:

```bash
./mvnw -pl cobble-state-bench -am clean package exec:exec \
  -Dbenchmarks="io.cobble.flink.state.benchmark.ValueStateBenchmark.*"
```

## Run with the uber jar

First build the jar:

```bash
./mvnw -pl cobble-state-bench -am clean package
```

Then run it directly with standard JMH arguments:

```bash
java -jar cobble-state-bench/target/benchmarks.jar \
  -rf csv \
  -rff cobble-state-bench/target/benchmark-results/state-benchmarks.csv \
  "io.cobble.flink.state.benchmark.*"
```

You can also use normal JMH filters and parameters, for example:

```bash
java -jar cobble-state-bench/target/benchmarks.jar \
  -p backendType=ROCKSDB \
  "io.cobble.flink.state.benchmark.ValueStateBenchmark.valueGet"
```

## Defaults

The module defaults are:

| Setting | Default |
| --- | ---: |
| Warmup iterations | 10 |
| Measurement iterations | 10 |
| Forks | 3 |

The Maven entrypoint follows the upstream `flink-benchmarks` pattern and additionally sets `-w 1s`, `-r 1s`, and `-rf csv`, writing results to `cobble-state-bench/target/benchmark-results/state-benchmarks.csv`. When running the uber jar directly, timing follows normal JMH defaults unless you pass `-w` / `-r` explicitly.

## Maven properties

The Maven entrypoint can be adjusted with these properties:

| Property | Default | Meaning |
| --- | --- | --- |
| `benchmarks` | `io.cobble.flink.state.benchmark.*` | Benchmark regex passed to JMH |
| `benchmark.result.format` | `csv` | JMH result format |
| `benchmark.result.file` | `${project.build.directory}/benchmark-results/state-benchmarks.csv` | Result file path |
| `benchmark.warmup.iterations` | `10` | JMH warmup iterations |
| `benchmark.measurement.iterations` | `10` | JMH measurement iterations |
| `benchmark.iteration.seconds` | `1` | Warmup / measurement iteration time in seconds |
| `benchmark.forks` | `3` | JMH fork count |
