<p align="center"><img src="https://github.com/cobble-project/cobble/raw/main/logo.png" width="60%" alt="Cobble logo" /></p>

Cobble-flink integrates [Cobble](https://github.com/cobble-project/cobble) with
[Apache Flink®](https://flink.apache.org/), so you can use Cobble as a Flink
state backend, SQL source, and SQL sink.

## Features

Cobble Flink currently provides:

- a **state backend** for stateful Flink jobs
- a **SQL source** for reading Cobble data in Flink SQL
- a **SQL sink** for writing Flink SQL results into Cobble
- a bundled **runtime jar** for Flink cluster deployment
- a **web monitor** for inspecting checkpoint and sink snapshots

For complete details, see the [documentation](https://cobble-project.github.io/cobble-flink/).

## Download and Versioning

You can get Cobble Flink artifacts from a Maven repository, or build them from
source if needed.

When choosing a version, make sure it matches both your Cobble version and your
Flink minor version.

The current release prefix is:

```text
0.2.0-1
```

Append the Flink compatibility suffix shown in the table below, for example:

```text
0.2.0-1-flink-1.17
```

Artifact versions vary when Flink breaks binary APIs. Use the matrix below to
choose the `<version>` value for each dependency in your `pom.xml` or for the
runtime jar you copy into Flink `lib/`:

| Flink cluster version | Dist bundle jar version | State dependency version | Sink dependency version | Source dependency version |
| --- | --- | --- | --- | --- |
| 1.17, 1.18 | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` |
| 1.19, 1.20 | `0.2.0-1-flink-1.19` | `0.2.0-1-flink-1.19` | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` |
| 2.0 and above | `0.2.0-1-flink-2.0` | `0.2.0-1-flink-2.0` | `0.2.0-1-flink-2.0` | `0.2.0-1-flink-1.17` |

ArtifactIds stay the same across Flink versions. Choose the artifactId from
the section you are using, then choose the `<version>` from the table.

## Setup

For most users, the simplest setup is to download the runtime jar and put it
into the Flink distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download the runtime jar artifact that matches your Flink version and copy it
into Flink `lib/`. For example, on Flink 1.17 or 1.18:

```bash
cp cobble-flink-dist-0.2.0-1-flink-1.17.jar "$FLINK_HOME/lib/"
```

If you are developing from source instead of downloading from Maven, you can
build the distribution jar locally:

```bash
./mvnw -pl :cobble-flink-dist -am package -DskipTests
cp cobble-dist/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 1.19 or 1.20, build `cobble-dist-flink-1.19`:

```bash
./mvnw -pl cobble-common,cobble-state-flink-1.19,cobble-sink,cobble-source,cobble-dist-flink-1.19 \
  package -DskipTests
cp cobble-dist-flink-1.19/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 2.0 and above, build `cobble-dist-flink-2.0`:

```bash
./mvnw -pl cobble-common,cobble-state-flink-2.0,cobble-sink-flink-2.0,cobble-source,cobble-dist-flink-2.0 \
  package -DskipTests
cp cobble-dist-flink-2.0/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

These artifacts can be built in the same reactor; no Maven profile switch is
required.

### Option B: use job-side Maven dependencies

When writing a Flink job, add the dependencies you actually use.

The following example is for a Flink 1.19 or 1.20 job that uses all three
parts. The state backend uses the 1.19-compatible artifact version, while sink
and source can use the 1.17-compatible artifact version:

```xml
<dependencies>
  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-state</artifactId>
    <version>0.2.0-1-flink-1.19</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-source</artifactId>
    <version>0.2.0-1-flink-1.17</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-sink</artifactId>
    <version>0.2.0-1-flink-1.17</version>
  </dependency>
</dependencies>
```

Typical choices:

- stateful DataStream job: `cobble-flink-state`
- SQL read job: `cobble-flink-source`
- SQL write job: `cobble-flink-sink`

### Configure Flink and run your job

Make sure to configure Flink as described in the next section, then you can run your job as usual.
Cobble will automatically be used for state management and SQL source/sink based on your configuration.

## Web Monitor

`cobble-flink-monitor` starts a read-only web UI for inspecting Cobble data in
Flink checkpoints or normal Cobble sink/table snapshots. It can list available
checkpoints/snapshots, select `latest` or a concrete snapshot, choose operators
for checkpoint sources, scan key/value rows, and track selected rows. For
Cobble state backend checkpoints, it can display schema-aware state and timer
parts such as state key, map key, list elements, timer timestamp, timer key, and
decoded values when metadata is available.

Build it with:

```bash
./mvnw -pl cobble-flink-monitor -am package -DskipTests
```

Start it with an optional initial source:

```bash
java -jar cobble-flink-monitor/target/cobble-flink-monitor-*.jar \
  --checkpoint s3://bucket/path/to/checkpoints \
  --flink-conf "$FLINK_HOME/conf" \
  --port 18088
```

If `--checkpoint` is omitted, open the UI first and use the `Datasource` page to
enter a checkpoint root, a concrete `chk-*` directory, or a Cobble sink/table
path. `--flink-conf` is optional and is useful when checkpoint data lives on a
remote filesystem configured through Flink.

## Get Started

### Flink's State Backend

For stateful jobs, Cobble can be used as the Flink state backend. To use it, set the following in `flink-conf.yaml`:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
```

If you originally used another HA mode, move it to `cobble.ha.delegate.type`.

Example:

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
[state backend docs](https://cobble-project.github.io/cobble-flink/state-backend/#restore-from-a-rocksdb-canonical-savepoint)
for the full guide.

### Flink's Sink

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

### Flink's Source

For SQL reads, use:

- `connector='cobble'`
- `path`
- `scan.checkpoint-id`
- `scan.mode`

Example:

```sql
CREATE TABLE source_tbl (
  phase STRING,
  id BIGINT,
  v BIGINT,
  PRIMARY KEY (phase, id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'hdfs:///tmp/cobble-table',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

Lookup join example:

```sql
SELECT o.order_id, o.id, d.name, d.score
FROM orders AS o
LEFT JOIN source_tbl FOR SYSTEM_TIME AS OF o.pt AS d
ON o.id = d.id;
```

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Notice

The [Apache Flink®](https://flink.apache.org/) is registered trademarks of The Apache Software Foundation in the United States and other countries.
