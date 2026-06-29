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

Download the released runtime jar from a Maven repository and put it into
Flink's `lib/` directory. Use the version matrix in
[Getting Started](../getting-started/) to choose the artifact that matches your
Flink version. For example, on Flink 1.19 or 1.20:

```bash
cp cobble-flink-dist-0.2.0-1-flink-1.19.jar "$FLINK_HOME/lib/"
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

Use `cobble-flink-state` for every supported Flink version. Select the
version value from the state column in [Getting Started](../getting-started/).

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
| `state.backend.cobble.compaction.read-ahead.enabled` | `true` | Whether Cobble compaction read-ahead is enabled. |
| `state.backend.cobble.compaction.remote.addr` | none | Address (`host:port`) of a Cobble remote compactor. When unset, compaction runs locally in the TaskManager. |
| `state.backend.cobble.compaction.remote.timeout` | `300s` | Timeout for a single remote compaction request. |
| `state.backend.cobble.compaction.threads` | `4` | Number of Cobble compaction worker threads on the writer (TaskManager) side. When compaction runs locally this is the local compaction thread pool; when remote compaction is enabled this sizes the writer's remote-compaction submission runtime. The remote compactor process has its own worker pool, configured by `compaction_threads` in its Cobble config (see [Remote Compaction](#remote-compaction)). |
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

## Remote Compaction

By default, Cobble runs compaction in the TaskManager process. You can offload
compaction CPU and I/O to a separate remote compactor by setting
`state.backend.cobble.compaction.remote.addr`.

### Start a compactor

The compactor needs a Cobble config file that points at the same storage used by
the TaskManagers. For a local test this can be a shared filesystem path. For
object storage or HDFS, configure the same volume endpoints and credentials that
the Flink job uses.

You can start the compactor from Rust:

```rust
use cobble::{Config, RemoteCompactionServer};

fn main() -> cobble::Result<()> {
    let config = Config::from_path("cobble-compactor.yaml")?;
    let server = RemoteCompactionServer::new(config)?;
    server.serve("0.0.0.0:18888")
}
```

You can also start it from the [cobble-java](https://repo1.maven.org/maven2/io/github/cobble-project/cobble/0.2.1/) artifact, which bundles
`cobble-cli`:

```bash
java -jar cobble-0.2.1.jar remote-compactor \
  --config ./cobble-compactor.yaml \
  --bind 0.0.0.0:18888
```

If you already use a Cobble Flink dist bundle, the same bundled CLI entrypoint
is available there too. Pick the dist jar that matches your Flink version:

```bash
java -jar cobble-flink-dist-0.2.0-1-flink-1.17.jar remote-compactor \
  --config ./cobble-compactor.yaml \
  --bind 0.0.0.0:18888
```

Or start the bundled CLI from Java code:

```java
import io.cobble.CobbleCli;
import io.cobble.CobbleCliProcess;
import java.nio.file.Paths;

try (CobbleCliProcess process =
        CobbleCli.startRemoteCompactor(Paths.get("cobble-compactor.yaml"), "0.0.0.0:18888")) {
    process.waitFor();
}
```

### Configure TaskManagers

Point the Flink state backend to the compactor address:

```yaml
state.backend.cobble.compaction.remote.addr: 127.0.0.1:18888
state.backend.cobble.compaction.remote.timeout: 30s
state.backend.cobble.compaction.threads: 2
state.backend.cobble.compaction.read-ahead.enabled: true
```

Leave `state.backend.cobble.compaction.remote.addr` unset to keep compaction
local in each TaskManager. A blank value is treated the same as unset.

`state.backend.cobble.compaction.threads` is a TaskManager-side setting. When
remote compaction is disabled, it sizes the local compaction pool. When remote
compaction is enabled, it sizes the TaskManager's remote-submission runtime. The
compactor process has its own worker pool, configured by `compaction_threads` in
the compactor's Cobble config.

Use the same Cobble version for the compactor and the TaskManagers. Upgrade them
together when changing Cobble versions.

## Restore From A RocksDB Canonical Savepoint

This section explains how to migrate a stateful Flink job from the RocksDB state
backend to the Cobble state backend by restoring from a RocksDB **canonical
savepoint**.

Cobble can **restore from** canonical savepoints, but it does **not create**
canonical savepoints. After the restore, the job continues with regular Cobble
checkpoints.

### When to use this

- You have an existing RocksDB-backed job and want to move it onto Cobble.
- You keep the job logic and state descriptors unchanged.
- You may also change parallelism at the same time.

The restore path has been tested with real RocksDB canonical savepoints, covering
rescale and both event-time and processing-time timers.

### Create the canonical savepoint

Take a canonical savepoint from the running RocksDB-backed job:

```bash
bin/flink savepoint --type canonical <job-id> <savepoint-dir>
```

`--type canonical` writes a backend-agnostic format that Cobble can import.
Confirm your Flink version's exact savepoint command syntax if it differs.

### Switch the state backend

Point Flink at the Cobble state backend. Use the `flink-conf.yaml` keys described
earlier in [Configure `flink-conf.yaml`](#configure-flink-confyaml):

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
state.checkpoints.dir: hdfs:///user/you/checkpoints
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
```

Keep the job's state descriptors exactly as they were. The restored state is
matched by state name, so renaming or retyping a descriptor prevents a match.

### Restore with CLAIM

Start the job from the savepoint using Flink `CLAIM` restore mode:

```bash
bin/flink run \
  -s <savepoint-path> \
  -restoreMode CLAIM \
  -p <new-parallelism> \
  <job-jar>
```

Cobble restore and rescale currently support **only** Flink `CLAIM` restore mode.
`NO_CLAIM` and `LEGACY` are not supported.

`-p` can stay the same as the original job or change. Rescale restore is
supported: Flink narrows the canonical savepoint handles to each target
subtask's key-group range before Cobble imports the rows. A short note on the
mechanics — Flink assigns every key to a key group, and each subtask owns a
range of key groups, so rescaling is just re-slicing the same key groups across
a different number of subtasks.

### Validate after restore

1. Let the job process a small amount of data and confirm results look right.
2. Wait for the first Cobble checkpoint to complete.
3. Optional: Open the checkpoint root in the [web monitor](../web-monitor/) and inspect the
   relevant state and timer rows.

### Supported state

Cobble imports the following state from a canonical savepoint:

| Flink state | Canonical restore |
| --- |-------------------|
| `ValueState` | supported         |
| `ReducingState` | supported         |
| `AggregatingState` | supported         |
| Event-time timers | supported         |
| Processing-time timers | supported         |
| `FoldingState` | not supported     |
| Operator state | not supported     |
| Broadcast state | not supported     |

### Serializer compatibility

Cobble accepts a restored serializer only when Flink reports it as **compatible
as-is**. If Flink reports that migration is required, that a reconfigured
serializer is required, or that the serializers are incompatible, Cobble rejects
the restore. Keep serializers stable across the migration, or rework state
before taking the savepoint.

### Common restore errors

Cobble detects problems as early as possible. Metadata that can be checked
without reading rows is validated before any state is written; row-level
corruption is caught during import. Either way, a failed restore does not leave
a half-imported, usable Cobble database behind.

- **Unsupported state type** — the savepoint contains `FoldingState`, operator
  state, broadcast state, or an otherwise unrecognized state kind. The error
  names the state and the rejected type.
- **Serializer incompatibility** — a state, namespace, or timer serializer is
  not compatible as-is. The error names the state, the role (for example value
  serializer or timer element serializer), and the canonical and runtime
  serializer classes.
- **Malformed or corrupt savepoint entries** — a row or timer entry cannot be
  decoded. The error includes the state name and key group when available.
- **Non-CLAIM restore mode** — restoring with `NO_CLAIM` or `LEGACY` is not
  supported. Use `CLAIM`.
