# cobble-flink-monitor

`cobble-flink-monitor` starts a read-only web monitor for Cobble snapshots produced by the
Cobble Flink state backend. It uses the JDK built-in HTTP server and serves both the API and
the static UI from one Java process.

For usage, please refer to the [Web Monitor](https://cobble-project.github.io/cobble-flink/latest/web-monitor/).

## Build

```bash
./mvnw -pl cobble-flink-monitor -am package -DskipTests
```

## Run

Start the monitor and open the UI. `--checkpoint` is optional; when it is omitted, use the
`Datasource` page's `Open new path` button to choose a local directory, or provide an initial path
with `--checkpoint`.

The data source can be either a Flink checkpoint root or a normal Cobble table/sink path. The
server auto-detects the layout. For checkpoint roots, it scans `chk-*`, Cobble HA global snapshots,
and shared shard snapshots, then lets the UI choose `latest`, a concrete checkpoint, and the
operator. For normal Cobble data sources, it reads `snapshot/SNAPSHOT-*` and exposes the latest or a
concrete Cobble snapshot.

```bash
java -jar cobble-flink-monitor/target/cobble-flink-monitor-*-SNAPSHOT.jar
```

You can also provide an initial source:

```bash
java -jar cobble-flink-monitor/target/cobble-flink-monitor-*-SNAPSHOT.jar \
  --checkpoint file:///path/to/checkpoints
```

Passing a concrete `chk-*` directory also works; the monitor uses its parent as the checkpoint root:

```bash
java -jar cobble-flink-monitor/target/cobble-flink-monitor-*-SNAPSHOT.jar \
  --checkpoint file:///path/to/checkpoints/chk-42
```

Useful options:

```text
--bind 127.0.0.1
--port 8088
--checkpoint file:///path/to/checkpoints-or-cobble-table
--flink-conf /path/to/flink/conf
--total-buckets 32768
--inspect-default-limit 100
--inspect-max-limit 1000
```

`--flink-conf` is optional. When it is present, the monitor loads Flink's filesystem
configuration before scanning checkpoints, so remote checkpoint paths such as S3, OSS, Azure,
GCS, and Hadoop-compatible filesystems can use the same credentials and options as a Flink
cluster. The shaded jar includes the common Flink filesystem plugins.

The monitor exposes:

- `GET /healthz`
- `POST /api/v1/discovery`
- `POST /api/v1/sessions`
- `GET /api/v1/sessions/{id}/overview`
- `POST /api/v1/sessions/{id}/scan`
- `POST /api/v1/sessions/{id}/lookup`
- `DELETE /api/v1/sessions/{id}`

Open a session for a checkpoint or Table snapshot, then use its target IDs to scan or look up
rows. Overview returns the available schemas and SQL source examples. Close the session when
finished to release its readers. See the [HTTP API guide](../docs/web-monitor/api/index.md)
for request bodies and examples.
