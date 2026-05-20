Cobble-flink integrates [Cobble](https://github.com/cobble-project/cobble) with
[Apache Flink®](https://flink.apache.org/), so you can run Flink workloads on Cobble state and
storage semantics.

## Modules

- `cobble-state`: Cobble Flink state backend and HA integration.
- `cobble-sink`: Cobble Flink SQL sink connector.
- `cobble-source`: Cobble Flink SQL source connector.
- `cobble-dist`: distribution jar that bundles `cobble-state`, `cobble-sink`, and `cobble-source` for deployment.
- `cobble-state-bench`: benchmark module for Cobble state backend scenarios.

## Setup

1. Build jars:
   ```bash
   ./mvnw -DskipTests package
   ```
2. Copy one deployment jar into your Flink `lib/` directory. `cobble-dist` contains the state backend, sink, and source connector:
   ```bash
   cp cobble-dist/target/cobble-dist-*.jar $FLINK_HOME/lib/
   ```
3. Start the Flink cluster.

## Get Started

### cobble-state

For a minimal run, configure Cobble with the full factory class names:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
```

Cobble HA replaces Flink's original HA factory with a wrapper. Move your original
`high-availability.type` value into `cobble.ha.delegate.type`.

For example, if you originally used Kubernetes HA:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
cobble.ha.delegate.type: kubernetes
```

If your original HA was a custom factory, set `cobble.ha.delegate.type` to that fully qualified
factory class name instead.

### cobble-sink

For a minimal SQL sink table, configure:

- `connector='cobble'`
- `path` (required; relative paths are normalized to `file://` absolute URIs)
- `bucket` (required)
- `sink.parallelism` (required)

Example:

```sql
CREATE TABLE sink_tbl (
  k BIGINT,
  v BIGINT,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'file:///tmp/cobble-table',
  'bucket' = '16',
  'sink.parallelism' = '4'
);
```

### cobble-source

For a minimal SQL source table, configure:

- `connector='cobble'`
- `path` (required; relative paths are normalized to `file://` absolute URIs)
- `bucket` (optional; if omitted, the source infers total bucket count from the snapshot metadata)
- `scan.checkpoint-id` (`latest` or a positive checkpoint id; default `latest`)
- `scan.mode` (`batch` or `streaming`; default `batch`)
- `scan.poll-interval-ms` (used by `latest + streaming`; default `1000`)

The source requires a `PRIMARY KEY` and at least one non-primary-key column in the table schema.

Example:

```sql
CREATE TABLE source_tbl (
  phase STRING,
  id BIGINT,
  v BIGINT,
  PRIMARY KEY (phase, id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'file:///tmp/cobble-table',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

Current source semantics:

- `latest + batch`: reads the current latest completed snapshot image once, then finishes.
- `latest + streaming`: reads the current latest completed snapshot image, keeps polling for newer snapshots, and stays unbounded.
- The current table changelog mode is `INSERT`-only. The source emits `RowKind.INSERT` records only; it does not yet emit `UPDATE_AFTER` / `DELETE`.

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Notice

The [Apache Flink®](https://flink.apache.org/) is registered trademarks of The Apache Software Foundation in the United States and other countries. 
