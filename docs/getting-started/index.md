---
title: Getting Started
nav_order: 3
---

# Getting Started

This page gives the shortest path to start using Cobble Flink.

## Version Selection

Cobble Flink artifacts should be selected with the correct version naming rule:

```text
${cobble-version}-{patch-version}-flink-{flink-minor-version}
```

Example:

```text
0.4.0-1-flink-1.17
```

Use the matrix below to choose the `<version>` value for each dependency in
your `pom.xml` or for the runtime jar you copy into Flink `lib/`:

| Flink cluster version | Dist bundle jar version | State dependency version | Sink dependency version | Source dependency version |
| --- | --- | --- | --- | --- |
| 1.17, 1.18 | `0.4.0-1-flink-1.17` | `0.4.0-1-flink-1.17` | `0.4.0-1-flink-1.17` | `0.4.0-1-flink-1.17` |
| 1.19, 1.20 | `0.4.0-1-flink-1.19` | `0.4.0-1-flink-1.19` | `0.4.0-1-flink-1.17` | `0.4.0-1-flink-1.17` |
| 2.0 | `0.4.0-1-flink-2.0` | `0.4.0-1-flink-2.0` | `0.4.0-1-flink-2.0` | `0.4.0-1-flink-1.17` |
| 2.1 and above | `0.4.0-1-flink-2.1` | `0.4.0-1-flink-2.1` | `0.4.0-1-flink-2.0` | `0.4.0-1-flink-1.17` |

The dist bundle jar is a single artifact that contains all three parts, it is the recommended way to use Cobble Flink.
The other three parts are separate artifacts that can be used as job-side Maven dependencies.
ArtifactIds stay the same across Flink versions. Choose the artifactId from
the section you are using, then choose the `<version>` from the table.

## Development Monitor Versions

The matrix above remains the published `0.4.0-1` release matrix. The current
`0.6.0` development tree builds the following standalone monitor variants;
these new variants are not yet published releases:

| Checkpoint producer line | Bundled Flink runtime | Build module | Development monitor version |
| --- | --- | --- | --- |
| 1.17 | 1.17.2 | `cobble-flink-monitor` | `0.6.0-1-flink-1.17-SNAPSHOT` |
| 1.19 | 1.19.3 | `cobble-flink-monitor-flink-1.19` | `0.6.0-1-flink-1.19-SNAPSHOT` |
| 2.0 | 2.0.2 | `cobble-flink-monitor-flink-2.0` | `0.6.0-1-flink-2.0-SNAPSHOT` |
| 2.1 | 2.1.1 | `cobble-flink-monitor-flink-2.1` | `0.6.0-1-flink-2.1-SNAPSHOT` |

All use artifactId `cobble-flink-monitor` and the same source code and web UI.
Match the monitor to the checkpoint producer line; the 1.17 runtime cannot
read all newer `_metadata` formats. Flink 2.2 and later are outside this
development monitor matrix. See [Web Monitor](../web-monitor/) for building.

Make sure the version matches:

- the Cobble version you want to use
- the Flink minor version of your cluster
- the artifact form you choose to use

## Flink Cluster Configuration

Use the configuration file and checkpoint directory key for your Flink version:

| Flink version | Cluster configuration file | Primary checkpoint directory key |
| --- | --- | --- |
| 1.17, 1.18 | `$FLINK_HOME/conf/flink-conf.yaml` | `state.checkpoints.dir` |
| 1.19 | `$FLINK_HOME/conf/config.yaml` by default; legacy `flink-conf.yaml` is supported and wins when both files exist | `state.checkpoints.dir` |
| 1.20 | `$FLINK_HOME/conf/config.yaml` by default; legacy `flink-conf.yaml` is supported and wins when both files exist | `execution.checkpointing.dir` (`state.checkpoints.dir` is a deprecated alias) |
| 2.0 and above | `$FLINK_HOME/conf/config.yaml` only | `execution.checkpointing.dir` (`state.checkpoints.dir` is a deprecated alias) |

`state.backend.type`, `high-availability.type`, `env.java.opts.all`, and
`env.java.home` use the same keys in every supported version. Only the
checkpoint directory key changes at Flink 1.20.

## Setup

For most users, the recommended setup is to download the runtime jar into the
Flink distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download the released runtime jar artifact from the table above and place it in
the Flink distribution's `lib/` directory. For example, on Flink 1.17 or 1.18:

```bash
export FLINK_HOME=/path/to/flink-1.17.x
cp cobble-flink-dist-0.4.0-1-flink-1.17.jar "$FLINK_HOME/lib/"
```

If you are not using a released jar yet and want to build from source, you can
build the distribution jar locally:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl :cobble-flink-dist -am package -DskipTests

cp cobble-dist/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 1.19 or 1.20, build `cobble-dist-flink-1.19`:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl cobble-common,cobble-state-flink-1.19,cobble-sink,cobble-source,cobble-dist-flink-1.19 \
  package -DskipTests

cp cobble-dist-flink-1.19/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 2.0, build `cobble-dist-flink-2.0`:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl cobble-common,cobble-state-flink-2.0,cobble-sink-flink-2.0,cobble-source,cobble-dist-flink-2.0 \
  package -DskipTests

cp cobble-dist-flink-2.0/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 2.1 and above, build `cobble-dist-flink-2.1`:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl cobble-common,cobble-state-flink-2.1,cobble-sink-flink-2.0,cobble-source,cobble-dist-flink-2.1 \
  package -DskipTests

cp cobble-dist-flink-2.1/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

These runtime jar artifacts can be built in the same reactor; no Maven profile
switch is required.

For normal users, the important point is simple: the Flink cluster should use
the bundled `dist` jar.

### Option B: use job-side Maven dependencies

To package Cobble with your Flink job, add the dist bundle that matches the
Flink cluster version. For Flink 1.19 or 1.20:

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-dist</artifactId>
  <version>0.4.0-1-flink-1.19</version>
</dependency>
```

The dist bundle includes the state backend, SQL source, and SQL sink.

Alternatively, add only the individual dependencies your job uses:

The following example is for a Flink 1.19 or 1.20 job that uses all three
parts. The state backend uses the 1.19-compatible artifact version, while sink
and source can use the 1.17-compatible artifact version:

```xml
<dependencies>
  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-state</artifactId>
    <version>0.4.0-1-flink-1.19</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-source</artifactId>
    <version>0.4.0-1-flink-1.17</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-sink</artifactId>
    <version>0.4.0-1-flink-1.17</version>
  </dependency>
</dependencies>
```

Common choices:

- stateful DataStream job: `cobble-flink-state`
- SQL read job: `cobble-flink-source`
- SQL write job: `cobble-flink-sink`
- mixed usage: add multiple dependencies

### Configure Flink if you use the state backend

If you want to use Cobble as the Flink state backend, add these shared settings
to the [cluster configuration file](#flink-cluster-configuration) for your
Flink version:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
state.backend.cobble.localdir: /tmp/flink-cobble/local
state.backend.cobble.memory.managed: true

# Recommended, optional: materialize checkpoint sidecars for faster monitor/source reads.
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
```

Then choose one primary checkpoint directory setting from the table above:

```yaml
# Flink 1.17, 1.18, or 1.19
state.checkpoints.dir: hdfs:///user/you/checkpoints
```

For Flink 1.20 or 2.0+, use this instead:

```yaml
execution.checkpointing.dir: hdfs:///user/you/checkpoints
```

The Cobble HA wrapper is recommended when you want sidecars materialized as checkpoints
complete, but it is not required for monitor or source access. When it is not enabled, the monitor and
state source can read Cobble payloads from Flink `_metadata` and rebuild a temporary read-only
view. To enable the wrapper, set `high-availability.type` to
`io.cobble.flink.state.CobbleHighAvailabilityServicesFactory`; move an existing HA type to
`cobble.ha.delegate.type`.

If you already use Kubernetes HA, for example:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
cobble.ha.delegate.type: kubernetes
```

Cobble restore and rescale currently support only Flink `CLAIM` restore mode.
`NO_CLAIM` and `LEGACY` are not supported.

Cobble can also restore a job from a **RocksDB canonical savepoint**, which is
the recommended way to migrate an existing RocksDB-backed job onto Cobble. Take
the savepoint with `flink savepoint --type canonical`, then start the job with
`-restoreMode CLAIM`. See the
[state backend docs](../state-backend/#restore-from-a-rocksdb-canonical-savepoint)
for the full guide.

Flink 2.0 deployments normally use Java 17. This does not imply that Java 11
cannot be used where the Flink distribution supports it.

Flink 1.19 and newer distributions normally already set `env.java.opts.all`
with the required `sun.nio.ch`, `java.lang`, and `java.util` access flags. Do
not replace that value with a shorter Cobble-specific value: preserve it and
append only flags that are missing. In the usual 1.19+ distribution setup, no
change is needed.

For Flink 1.17 or 1.18, add this single-line setting if those flags are not
already present:

```yaml
env.java.opts.all: "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
```

If multiple JDKs are installed, pin the runtime explicitly if needed:

```yaml
env.java.home: /path/to/your/jdk
```

If your job only uses source or sink, you do not need this state-backend
configuration.

### Start Flink and submit the job

1. If you chose the runtime-jar setup, put the Cobble jar into `$FLINK_HOME/lib`
2. Update the cluster configuration file for your Flink version if you use the state backend
3. Start the cluster with `$FLINK_HOME/bin/start-cluster.sh`
4. Submit your job with `$FLINK_HOME/bin/flink run ...`

## Write Results with the SQL Sink

For SQL writes, use:

- `connector='cobble'`
- `path`
- `bucket`
- `sink.parallelism`

Example:

```sql
CREATE TABLE sink_tbl (
  k BIGINT,
  v BIGINT,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'hdfs:///tmp/cobble-table',
  'bucket' = '16',
  'sink.parallelism' = '4'
);
```

## Read and Reuse Data with the SQL Source

Cobble source lets Flink SQL read data that is already stored in Cobble. There
are three source kinds:

- `source.kind='sink'` reads a Cobble table written by the Cobble SQL sink.
- `source.kind='state'` reads keyed state from a Flink checkpoint written by the
  Cobble state backend.
- `source.kind='raw'` reads any standard Cobble table root as raw bytes, without
  depending on sink or state schema. Useful for debugging and interoperability.

Use `source.kind='auto'` when the path layout is unambiguous, or set the kind
explicitly in production DDL. `auto` never selects `raw`; it must be set
explicitly.

For reading a Cobble sink table, use:

- `connector='cobble'`
- `path`
- `source.kind='sink'`
- `scan.checkpoint-id`
- `scan.mode`

Sink tables require a primary key and can also be used in temporal lookup joins:

```sql
CREATE TABLE source_tbl (
  phase STRING,
  id BIGINT,
  v BIGINT,
  PRIMARY KEY (phase, id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'sink',
  'path' = 'hdfs:///tmp/cobble-table',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

Lookup join example (`orders.pt` is a processing-time attribute):

```sql
SELECT o.order_id, o.phase, o.id, d.v
FROM orders AS o
LEFT JOIN source_tbl FOR SYSTEM_TIME AS OF o.pt AS d
ON o.phase = d.phase AND o.id = d.id;
```

For reading Cobble state backend checkpoints, use:

- `connector='cobble'`
- `path` pointing at the checkpoint root or a concrete `chk-*` directory
- `source.kind='state'`
- `state.name`
- `state.operator-id` when the checkpoint has more than one Cobble operator
- `state.kind` when you want an explicit validation hint
- `scan.checkpoint-id`
- `scan.mode`

State scan does not require a primary key. State lookup joins require a primary
key that contains the full exact lookup key. For `ValueState`, `ReducingState`,
and `AggregatingState`, that means the state key plus namespace when present.
For `MapState`, it means state key, namespace when present, and map key.

When the Cobble data was produced by Flink SQL, Cobble records schema metadata
so the source can expose semantic columns instead of raw key/value bytes. For
example, a SQL join state can be read with columns such as `customer_id`,
`order_id`, and `amount`, and a Cobble sink table can be read with its original
primary-key and value columns. See [Source](../source/) for the full DDL
patterns.

```sql
CREATE TABLE state_values (
  `key` INT,
  `value` INT,
  PRIMARY KEY (`key`) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'state',
  'path' = 'hdfs:///tmp/flink-checkpoints',
  'state.operator-id' = '<operator-id>',
  'state.name' = 'value-state',
  'state.kind' = 'value',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);

SELECT p.`key`, d.`value`
FROM probes AS p
LEFT JOIN state_values FOR SYSTEM_TIME AS OF p.pt AS d
ON p.`key` = d.`key`;
```

For reading any Cobble table root as raw bytes (no sink or state schema), use:

- `connector='cobble'`
- `path`
- `source.kind='raw'`
- `raw.columns` (required, e.g. `'0,1'`)
- `scan.checkpoint-id`
- `scan.mode`

The raw source requires a fixed two-column DDL: `key BYTES` and `columns
ARRAY<BYTES>`. It emits the raw Cobble row key and the selected structured value
columns as raw bytes, preserving nulls and arbitrary binary content. Lookup is
not supported.

```sql
CREATE TABLE raw_cobble (
  `key` BYTES,
  `columns` ARRAY<BYTES>
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'raw',
  'path' = 'hdfs:///tmp/cobble-table',
  'raw.columns' = '0,1',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);

SELECT `key`, `columns` FROM raw_cobble;
```

## Where To Go Next

- [State Backend](../state-backend/) if your job is stateful
- [Source](../source/) if you want to read from Cobble
- [Sink](../sink/) if you want to write into Cobble
