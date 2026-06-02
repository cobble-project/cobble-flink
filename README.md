Cobble-flink integrates [Cobble](https://github.com/cobble-project/cobble) with
[Apache Flink®](https://flink.apache.org/), so you can run Flink workloads on Cobble state and
storage semantics.

## Modules

- `cobble-flink-state`: Cobble Flink state backend and HA integration.
- `cobble-flink-sink`: Cobble Flink SQL sink connector.
- `cobble-flink-source`: Cobble Flink SQL source connector.
- `cobble-flink-dist`: distribution jar that bundles `cobble-flink-state`, `cobble-flink-sink`, and `cobble-flink-source` for deployment.
- `cobble-flink-state-bench`: benchmark module for Cobble state backend scenarios.
- `cobble-flink-tests`: end-to-end integration tests for Cobble Flink connectors and runtime flows.

## Setup

1. Build jars:
   ```bash
   ./mvnw -DskipTests package
   ```
2. Copy one deployment jar into your Flink `lib/` directory. `cobble-flink-dist` contains the state backend, sink, and source connector:
   ```bash
   cp cobble-dist/target/cobble-flink-dist-*.jar $FLINK_HOME/lib/
   ```
3. Start the Flink cluster.

## Get Started

### Flink's State Backend

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

Cobble restore and rescale currently support only Flink's `CLAIM` restore mode because restored
snapshot volumes are incorporated into the writable native DB lifecycle. `NO_CLAIM` and `LEGACY`
restore modes are not supported.

### Flink's Sink

For a minimal SQL sink table, configure:

- `connector='cobble'`
- `path` (required; relative paths are normalized to `file://` absolute URIs)
- `bucket` (required)
- `sink.parallelism` (required)

The sink uses primary-key upsert semantics. It materializes final state by `PRIMARY KEY`, applies
`INSERT` / `UPDATE_AFTER`, handles `DELETE`, and ignores paired `UPDATE_BEFORE` records.

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

### Flink's Source

For a minimal SQL source table, configure:

- `connector='cobble'`
- `path` (required; relative paths are normalized to `file://` absolute URIs)
- `bucket` (optional; if omitted, the source infers total bucket count from the snapshot metadata)
- `scan.checkpoint-id` (`latest` or a positive checkpoint id; default `latest`)
- `scan.mode` (`batch` or `streaming`; default `batch`)
- `scan.poll-interval-ms` (used by `latest + streaming`; default `1000`)

The source requires a `PRIMARY KEY` and at least one non-primary-key column in the table schema.
The same table can also be used as a lookup dimension table in Flink SQL. Lookup joins currently
require equality conditions on the full `PRIMARY KEY`.

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

Lookup join example:

```sql
SELECT o.order_id, o.id, d.name, d.score
FROM orders AS o
LEFT JOIN source_tbl FOR SYSTEM_TIME AS OF o.pt AS d
ON o.id = d.id;
```

Current source semantics:

- `latest + batch`: reads the current latest completed snapshot image once, then finishes.
- `latest + streaming`: reads the current latest completed snapshot image, keeps polling for newer snapshots, and stays unbounded.
- The current table changelog mode is `INSERT`-only. The source emits `RowKind.INSERT` records only; it does not yet emit `UPDATE_AFTER` / `DELETE`.

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Notice

The [Apache Flink®](https://flink.apache.org/) is registered trademarks of The Apache Software Foundation in the United States and other countries. 
