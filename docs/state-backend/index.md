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
  manifests. This makes restart and rescale more efficient when state is large. Rescale uses
  Cobble's default asynchronous adoption mode, so exported checkpoint files remain retained until
  Cobble has taken ownership.
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
cp cobble-flink-dist-0.4.0-1-flink-1.19.jar "$FLINK_HOME/lib/"
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

### Configure the Flink cluster configuration

Choose the configuration file and checkpoint directory key for your Flink
version in [Getting Started](../getting-started/#flink-cluster-configuration).
`state.backend.type`, `high-availability.type`, `env.java.opts.all`, and
`env.java.home` use the same keys in Flink 1.17 through 2.0.

Minimal example:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
state.backend.cobble.localdir: /tmp/flink-cobble/local
state.backend.cobble.memory.managed: true

# Recommended, optional: materialize checkpoint sidecars for faster monitor/source reads.
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
```

Add the version-appropriate checkpoint directory setting from Getting Started:
`state.checkpoints.dir` for Flink 1.17–1.19, or
`execution.checkpointing.dir` for Flink 1.20 and 2.0+.

The Cobble HA wrapper is recommended, not required. It materializes and maintains Cobble
sidecars as checkpoints complete, so the monitor and state source can open them directly. Without
the wrapper, those tools read Cobble payloads embedded in Flink `_metadata` and temporarily
materialize a read-only view, so checkpoints and native savepoints remain inspectable.

If you enable the wrapper and were already using another Flink HA mode, keep Cobble as
`high-availability.type` and move the old value into `cobble.ha.delegate.type`.

Example:

```yaml
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
cobble.ha.delegate.type: kubernetes
```

For JVM flags, follow the version-specific guidance in
[Getting Started](../getting-started/#configure-flink-if-you-use-the-state-backend).
In particular, preserve the existing `env.java.opts.all` value in Flink 1.19+
distributions and append only missing flags. If multiple JDKs are installed,
you can also pin the runtime:

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
| `state.checkpoints.dir` | none | Primary checkpoint storage key for Flink 1.17–1.19. |
| `execution.checkpointing.dir` | none | Primary checkpoint storage key for Flink 1.20 and 2.0+. `state.checkpoints.dir` remains a deprecated alias. |
| `high-availability.type` | none | Recommended: set this to `io.cobble.flink.state.CobbleHighAvailabilityServicesFactory` to materialize Cobble sidecars as checkpoints complete. |
| `env.java.opts.all` | none | Preserve Flink's existing value and append only missing Cobble JVM flags. |
| `env.java.home` | none | Optional JDK path; the same key is used by every supported Flink version. |

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
| `state.backend.cobble.memtable.type` | `adaptive` | Memtable implementation. `adaptive` (default) monitors access patterns and switches automatically: `vec` for write-heavy, `hash` for point-read-heavy, `skiplist` for mixed workloads. `skiplist` supports both point lookups and range scans; `hash` is available for point-lookup-only workloads. |
| `state.backend.cobble.compaction.policy` | `score_priority` | Compaction policy. Supported values are `round_robin`, `min_overlap`, and `score_priority`. |
| `state.backend.cobble.compaction.l0-file-limit` | `2` | Number of L0 files that triggers compaction. Higher values reduce compaction work but can increase state lookup and iteration cost. |
| `state.backend.cobble.compaction.write-stall-limit` | Derived by Cobble | Maximum combined immutable-memtable and L0-file pressure before Cobble blocks writes. The value must be at least `state.backend.cobble.compaction.l0-file-limit + 2`. When unset, Cobble derives `max(l0-file-limit + 2, 32)`. |
| `state.backend.cobble.compaction.read-ahead.enabled` | `true` | Whether Cobble compaction read-ahead is enabled. |
| `state.backend.cobble.compaction.mode` | `LOCAL` | Where compaction runs: `LOCAL` in the TaskManager, `REMOTE` over the compaction service protocol, or `DEDICATED` through shared storage. |
| `state.backend.cobble.compaction.remote.addr` | none | Address (`host:port`) of a Cobble remote compactor. Required in `REMOTE` mode. For compatibility, setting only this option also selects remote mode. |
| `state.backend.cobble.compaction.remote.timeout` | `300s` | Timeout for a single remote compaction request. |
| `state.backend.cobble.compaction.threads` | `4` | Number of Cobble compaction worker threads on the writer (TaskManager) side. When compaction runs locally this is the local compaction thread pool; when remote compaction is enabled this sizes the writer's remote-compaction submission runtime. The remote compactor process has its own worker pool, configured by `compaction_threads` in its Cobble config (see [Remote Compaction](#remote-compaction)). |
| `state.backend.cobble.compaction.dedicated.poll-interval` | `1s` | How often the TaskManager checks shared storage for dedicated-compaction results. |
| `state.backend.cobble.compaction.dedicated.orphan-min-age` | `5m` | Minimum age before abandoned dedicated-compaction output is eligible for cleanup. |
| `state.backend.cobble.async.read-threads` | `4` | Flink 2.0 only: worker threads used for asynchronous state reads. |
| `state.backend.cobble.async.write-threads` | `1` | Flink 2.0 only: worker threads used for asynchronous state writes. |
| `state.backend.cobble.sst.bloom-filter.enabled` | `true` | Whether SST bloom filters are enabled. |
| `state.backend.cobble.sst.bloom-filter.bits-per-key` | `10` | Bloom-filter density used when bloom filters are enabled. |
| `state.backend.cobble.sst.partitioned-index.enabled` | `true` | Whether partitioned SST index/filter blocks are enabled. |
| `state.backend.cobble.sst.read-metadata-cache.mode` | `EAGER` | Decoded SST metadata cache mode: `EAGER`, `LAZY`, or `OFF`. |
| `state.backend.cobble.sst.pinned-metadata.max-level` | `2` | Pin immutable top-level SST index and bloom-filter metadata outside the block cache for point reads, scans, and compactions. `-1` disables it; `0` pins L0; `N` pins L0 through LN. |
| `state.backend.cobble.sst.pinned-metadata.partitions.enabled` | `true` | Also pin second-level index and filter partitions for partitioned SST files. Set to `false` to reduce pinned memory at the cost of point-read performance. |
| `state.backend.cobble.value-separation.threshold` | `1kb` | Values larger than this threshold are separated into Cobble's value log. |
| `state.backend.cobble.vlog.low-priority-primary.enabled` | `false` | Place value-log files newly created or copied into primary on the lowest-priority tier. Existing primary replicas are not rebalanced or promoted by low-to-high primary backfill; if that tier is unavailable, writes fail rather than falling back to a higher tier. |
| `state.backend.cobble.direct-io.buffer-size` | `2kb` | Size of each pooled direct I/O buffer used by Cobble reads. |
| `state.backend.cobble.direct-io.pool-max-size` | `64` | Maximum number of pooled direct I/O buffers. |
| `state.backend.cobble.log.max-file-size` | `10mb` | Maximum size of one Cobble log file before rolling. |
| `state.backend.cobble.log.keep-files` | `3` | Number of Cobble log files to retain. |
| `state.backend.cobble.log.level` | `info` | Cobble native log level. Supported values are `trace`, `debug`, `info`, `warn`, `error`, and `off`. |
| `state.backend.cobble.snapshot.retention` | none | Automatically expire older snapshots after this many newer snapshots have completed. |
| `state.backend.cobble.localdir.primary-high-priority` | `true` | Use the local Cobble directory as the high-priority active-state volume. Checkpoint storage remains a low-priority fallback and stores metadata and snapshots. |

`EAGER` attaches decoded SST metadata when Cobble writes a new file and is the recommended default.
`LAZY` caches the metadata on the first read, while `OFF` rebuilds it for each reader.

For a first deployment, most users can start with just these keys:

- `state.backend.type`
- `state.checkpoints.dir` on Flink 1.17–1.19, or `execution.checkpointing.dir` on Flink 1.20+
- `high-availability.type`
- `state.backend.cobble.localdir`
- `state.backend.cobble.memory.managed`

Then tune memory, SST, logging, and snapshot-retention settings only if you
have a specific operational need.

## Usage Notes

- Cobble restore currently does **not** support Flink `NO_CLAIM` restore mode.
- Use Flink `CLAIM` when restoring from checkpoints.

## Remote Compaction

By default, Cobble runs compaction in the TaskManager process. Remote mode sends
each compaction request to a long-running compactor service.

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

You can also start it from the [cobble-java](https://repo1.maven.org/maven2/io/github/cobble-project/cobble/0.4.0/) artifact, which bundles
`cobble-cli`:

```bash
java -jar cobble-0.4.0.jar remote-compactor \
  --config ./cobble-compactor.yaml \
  --bind 0.0.0.0:18888
```

If you already use a Cobble Flink dist bundle, the same bundled CLI entrypoint
is available there too. Pick the dist jar that matches your Flink version:

```bash
java -jar cobble-flink-dist-0.4.0-1-flink-1.17.jar remote-compactor \
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
state.backend.cobble.compaction.mode: REMOTE
state.backend.cobble.compaction.remote.addr: 127.0.0.1:18888
state.backend.cobble.compaction.remote.timeout: 30s
state.backend.cobble.compaction.threads: 2
state.backend.cobble.compaction.read-ahead.enabled: true
```

Use `state.backend.cobble.compaction.mode: LOCAL` to keep compaction local in
each TaskManager.

`state.backend.cobble.compaction.threads` is a TaskManager-side setting. When
remote compaction is disabled, it sizes the local compaction pool. When remote
compaction is enabled, it sizes the TaskManager's remote-submission runtime. The
compactor process has its own worker pool, configured by `compaction_threads` in
the compactor's Cobble config.

Use the same Cobble version for the compactor and the TaskManagers. Upgrade them
together when changing Cobble versions.

## Dedicated Compaction

Dedicated mode coordinates through checkpoint storage instead of a network
request. A dedicated Flink job discovers Cobble databases, queues portable
compaction plans, and executes those plans in parallel. Results are published
through shared storage for the TaskManagers to apply.

Configure the state backend:

```yaml
state.backend.cobble.compaction.mode: DEDICATED
state.backend.cobble.compaction.dedicated.poll-interval: 1s
state.backend.cobble.compaction.dedicated.orphan-min-age: 5m
```

Dedicated mode requires shared checkpoint storage. Cobble writes active SSTs to
that shared volume so the external process can read them; the TaskManager local
directory remains a cache.

Create a Cobble config whose metadata/data volumes cover the shared checkpoint
prefix and contain the required storage credentials. The config file must be
available at the same path on the compaction job's TaskManagers.

Run the job with the matching dist jar. `--path` accepts a Cobble DB directory
or any parent directory/storage prefix. It discovers ordinary Cobble stores and
Flink state below that path using Cobble's own DB metadata. `--parallelism`
controls the number of concurrent compaction executors:

```bash
bin/flink run \
  -c io.cobble.flink.compaction.CobbleDedicatedCompactionJob \
  cobble-flink-dist-0.4.0-1-flink-1.17.jar \
  --config ./cobble-dedicated-compactor.yaml \
  --path s3://state-bucket/flink-checkpoints \
  --parallelism 4
```

The path must be absolute and must not contain credentials or storage options;
those come from `--config`. Monitoring always runs at parallelism one; plans
are shuffled to the configured executor parallelism. Planning and
execution are independent, and every executor revalidates the durable DB
observation before writing output, so queued stale plans are discarded safely.
Queued plans use absolute volume locations without credentials; each executor
loads credentials and storage options from `--config`.

The same dist jar can still run the standalone Cobble `compact` command when a
Flink-managed compaction service is not needed.

When state TTL is enabled, keep TaskManager and compactor host clocks synchronized.

## Restore From A RocksDB Canonical Savepoint

This section explains how to migrate a stateful Flink job from the RocksDB state
backend to the Cobble state backend by restoring from a RocksDB **canonical
savepoint**.

Cobble can both create and restore canonical savepoints on the synchronous
state path. Flink 2.0 async-state operators use native checkpoints instead.

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

Point Flink at the Cobble state backend. Use the cluster configuration keys
described earlier in [Configure the Flink cluster configuration](#configure-the-flink-cluster-configuration):

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
```

Also add the version-appropriate checkpoint directory key from
[Getting Started](../getting-started/#flink-cluster-configuration).

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
| `ListState` | supported         |
| `MapState` | supported, including present-null values |
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
