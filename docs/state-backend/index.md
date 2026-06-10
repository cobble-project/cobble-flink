---
title: State Backend
nav_order: 5
---

# State Backend

Use the Cobble state backend when your Flink job is stateful and you want Flink
state stored in Cobble. For more information about stateful stream processing,
please refer to [Flink's documentation](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/stateful-stream-processing/).

## Why Cobble Compared with RocksDB

RocksDB is a familiar choice for Flink state, but Cobble is a better fit when
you care about storage-compute separation and operational efficiency around
checkpoint recovery.

- **A better fit for shared storage and large state**
  Cobble is designed for storage-compute separation, so it works naturally when
  Flink compute runs on one side and state/checkpoint data lives on shared
  storage such as HDFS or object storage. Runtime files and checkpoint files
  work together more naturally, which reduces unnecessary data movement and
  makes Cobble a good fit for large-state workloads.
- **Faster recovery and rescale**
  When restoring to the same key-group range, Cobble can resume from the
  existing snapshot source directly. During rescale, it can rebuild only the
  key-group ranges needed by the new tasks from one or more checkpoint
  manifests. This makes restart and rescale more efficient when state is large.
- **Easier to consume and inspect state data**
  Once state data is in Cobble, it is easier to reuse Cobble's source
  capabilities for batch consumption or point lookup. In some scenarios, you
  can also inspect data through Cobble's web monitor.
- **Still easy to use locally**
  Even in local deployment, Cobble can keep the experience close to RocksDB, so
  it can still work well as a default state backend rather than only as a
  specialized option for remote storage.

## Installation and Setup

For most users, the easiest setup is to download the runtime jar into the Flink
distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download the released `cobble-flink-dist` jar from a Maven repository and put
it into Flink's `lib/` directory. Use the same version naming rule described in
[Getting Started](../getting-started/):

```bash
cp cobble-flink-dist-<version>.jar "$FLINK_HOME/lib/"
```

### Option B: use a job-side dependency

Add `cobble-flink-state` to your job if you prefer to package the backend with
the job itself:

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-state</artifactId>
  <version>${cobble.flink.version}</version>
</dependency>
```

Most users do not need this if the runtime jar is already present in Flink
`lib/`.

### Configure `flink-conf.yaml`

Minimal example:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
state.backend.cobble.localdir: /tmp/flink-cobble/local
state.backend.cobble.memory.managed: true

state.checkpoints.dir: hdfs:///user/you/checkpoints

high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
cobble.ha.delegate.type: NONE
```

If you were already using another Flink HA mode before enabling Cobble, keep
Cobble as `high-availability.type` and move the old value into
`cobble.ha.delegate.type`.

Example:

```yaml
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
cobble.ha.delegate.type: kubernetes
```

If Flink runs on Java 11 or newer, also add:

```yaml
env.java.opts.all: >-
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
```

If multiple JDKs are installed, you can also pin the runtime:

```yaml
env.java.home: /path/to/your/jdk
```

### Enable checkpointing in the job

The backend is configured in Flink, but the job still needs checkpointing:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(10_000L);
```

## Minimal DataStream Example

```java
DataStream<String> words = env.fromElements("cobble", "flink", "cobble");

words.keyBy(word -> word)
        .map(new RichMapFunction<String, String>() {
            private transient ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                countState =
                        getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("count", Long.class));
            }

            @Override
            public String map(String value) throws Exception {
                Long count = countState.value();
                long next = count == null ? 1L : count + 1L;
                countState.update(next);
                return value + ":" + next;
            }
        });
```

## Complete Configuration Reference

This section lists the main configuration keys you can set for the Cobble state
backend.

### Required Flink-side Settings

| Key | Default | Description |
| --- | --- | --- |
| `state.backend.type` | none | Set this to `io.cobble.flink.state.CobbleStateBackendFactory` to enable Cobble as the Flink state backend. |
| `state.checkpoints.dir` | none | The checkpoint storage location. This should point to shared storage that all relevant restore flows can access. |
| `high-availability.type` | none | Set this to `io.cobble.flink.state.CobbleHighAvailabilityServicesFactory` when using the Cobble state backend. |
| `env.java.opts.all` | none | Required JVM flags when running Flink on Java 11 or newer. |
| `env.java.home` | none | Optional JDK path if you want to pin the Java runtime used by Flink. |

### Cobble HA Wrapper Settings

| Key | Default | Description |
| --- | --- | --- |
| `cobble.ha.delegate.type` | none | The original Flink HA mode or HA factory to delegate to, for example `NONE`, `kubernetes`, or a factory class name. |
| `cobble.ha.delegate.none-mode` | `auto` | How `cobble.ha.delegate.type=NONE` should be interpreted. Supported values are `auto`, `standalone`, and `embedded`. Most users can keep `auto`. |

### Cobble Backend Settings

| Key | Default | Description |
| --- | --- | --- |
| `state.backend.cobble.localdir` | TaskManager working directory | Local directory used by Cobble on each TaskManager. |
| `state.backend.cobble.timer-service.factory` | `COBBLE` | Timer storage implementation used by the backend. Most users should keep the default. |
| `state.backend.cobble.memory.managed` | `true` | Whether Cobble derives memory from Flink managed memory. |
| `state.backend.cobble.memory.fixed-per-slot` | none | Fixed memory budget per slot. Use this when you want to override managed-memory-based sizing. |
| `state.backend.cobble.memory.memtable-buffer-ratio` | `0.5` | Fraction of the Cobble memory budget reserved for memtable buffers. |
| `state.backend.cobble.memory.memtable-buffer-count` | `2` | Number of in-memory memtable buffers. |
| `state.backend.cobble.memtable.type` | `hash` | Memtable implementation. Supported values are `hash`, `skiplist`, and `vec`. |
| `state.backend.cobble.compaction.policy` | `round_robin` | Compaction policy. Supported values are `round_robin`, `min_overlap`, and `score_priority`. |
| `state.backend.cobble.sst.bloom-filter.enabled` | `false` | Whether SST bloom filters are enabled. |
| `state.backend.cobble.sst.bloom-filter.bits-per-key` | `10` | Bloom-filter density used when bloom filters are enabled. |
| `state.backend.cobble.sst.partitioned-index.enabled` | `false` | Whether partitioned SST index/filter blocks are enabled. |
| `state.backend.cobble.value-separation.threshold` | `1kb` | Values larger than this threshold are separated into Cobble's value log. |
| `state.backend.cobble.direct-io.buffer-size` | `2kb` | Size of each pooled direct I/O buffer used by Cobble reads. |
| `state.backend.cobble.direct-io.pool-max-size` | `64` | Maximum number of pooled direct I/O buffers. |
| `state.backend.cobble.log.max-file-size` | `10mb` | Maximum size of one Cobble log file before rolling. |
| `state.backend.cobble.log.keep-files` | `3` | Number of Cobble log files to retain. |
| `state.backend.cobble.log.level` | `info` | Cobble native log level. Supported values are `trace`, `debug`, `info`, `warn`, `error`, and `off`. |
| `state.backend.cobble.snapshot.retention` | none | Automatically expire older snapshots after this many newer snapshots have completed. |
| `state.backend.cobble.localdir.primary-high-priority` | `false` | Whether the local Cobble directory stays a high-priority primary volume even when checkpoint storage is configured. |

For a first deployment, most users can start with just these keys:

- `state.backend.type`
- `state.checkpoints.dir`
- `high-availability.type`
- `cobble.ha.delegate.type`
- `state.backend.cobble.localdir`
- `state.backend.cobble.memory.managed`

Then tune memory, SST, logging, and snapshot-retention settings only if you
have a specific operational need.

## Usage Notes

- Cobble restore currently does **not** support Flink `NO_CLAIM` restore mode.
- Use Flink `CLAIM` when restoring from checkpoints.
