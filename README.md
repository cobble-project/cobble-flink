Cobble-flink integrates [Cobble](https://github.com/cobble-project/cobble) with
[Apache Flink®](https://flink.apache.org/), so you can run Flink workloads on Cobble state and
storage semantics.

## Modules

- `cobble-state`: Cobble Flink state backend and HA integration.
- `cobble-sink`: Cobble Flink SQL sink connector.
- `cobble-dist`: distribution jar that bundles `cobble-state` and `cobble-sink` for deployment.
- `cobble-state-bench`: benchmark module for Cobble state backend scenarios.

## Setup

1. Build jars:
   ```bash
   ./mvnw -DskipTests package
   ```
2. Copy one deployment jar into your Flink `lib/` directory. `cobble-dist` contains all the package:
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

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Notice

The [Apache Flink®](https://flink.apache.org/) is registered trademarks of The Apache Software Foundation in the United States and other countries. 
