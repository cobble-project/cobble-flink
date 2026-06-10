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

For complete details, see the [documentation](https://cobble-project.github.io/cobble-flink/).

## Download and Versioning

You can get Cobble Flink artifacts from a Maven repository, or build them from
source if needed.

When choosing a version, make sure it matches both your Cobble version and your
Flink minor version.

The current version naming rule is:

```text
${cobble-version}-{patch-version}-flink-{flink-minor-version}
```

Example:

```text
0.2.0-1-flink-1.17
```

## Setup

For most users, the simplest setup is to download the runtime jar and put it
into the Flink distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download `cobble-flink-dist` from your Maven repository and copy it into Flink
`lib/`:

```bash
cp cobble-flink-dist-<version>.jar "$FLINK_HOME/lib/"
```

If you are developing from source instead of downloading from Maven, you can
build the distribution jar locally:

```bash
./mvnw -DskipTests package
cp cobble-dist/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

### Option B: use job-side Maven dependencies

When writing a Flink job, depend on the modules you actually use.

```xml
<properties>
  <cobble.flink.version>${cobble-version}-{minor-version}-flink-{flink-minor-version}</cobble.flink.version>
</properties>

<dependencies>
  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-state</artifactId>
    <version>${cobble.flink.version}</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-source</artifactId>
    <version>${cobble.flink.version}</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-sink</artifactId>
    <version>${cobble.flink.version}</version>
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
