---
title: Sink
nav_order: 7
---

# Sink

Use the Cobble sink when you want Flink SQL results written into a Cobble
table.

## Why Use Cobble Sink

Cobble sink is useful when you want a Flink SQL result table to be materialized
directly into Cobble.

- **Write Flink SQL results directly into Cobble**
  This is a straightforward way to persist keyed SQL results without adding a
  separate export step.
- **Natural fit for upsert-style result tables**
  The sink writes by primary key, so it works well for materialized tables that
  are updated over time.
- **Easy to pair with Cobble source**
  Data written by the sink can later be consumed again through Cobble source,
  including batch reading and lookup use cases.

## Installation and Setup

For most users, the easiest setup is to download the runtime jar into the Flink
distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download the released runtime jar from a Maven repository and put it into
Flink's `lib/` directory. Use the version matrix in
[Getting Started](../getting-started/) to choose the artifact that matches your
Flink version. For example, on Flink 2.0 and above:

```bash
cp cobble-flink-dist-0.2.0-1-flink-2.0.jar "$FLINK_HOME/lib/"
```

### Option B: use a job-side dependency

Add `cobble-flink-sink` to your job if you prefer to package the connector with
the job itself:

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-sink</artifactId>
  <version>${cobble.flink.version}</version>
</dependency>
```

Use `cobble-flink-sink` for every supported Flink version. Select the
version value from the sink column in [Getting Started](../getting-started/).

Most users do not need this if the runtime jar is already present in Flink
`lib/`.

### Define the sink table

The Cobble sink uses:

- `connector = 'cobble'`
- a required `path`
- a required `bucket`
- a required `sink.parallelism`
- a required `PRIMARY KEY`
- at least one non-primary-key column

## Minimal SQL Example

```sql
CREATE TABLE cobble_sink (
  id BIGINT,
  name STRING,
  score INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'file:///tmp/cobble-table',
  'bucket' = '16',
  'sink.parallelism' = '4',
  'snapshot.retention' = '2'
);
```

Then write into it:

```sql
INSERT INTO cobble_sink
SELECT id, name, score
FROM upstream_result;
```

## Remote storage

Set `path` to the table URI and provide any required access options in the
table definition. Keep credentials out of the URI:

```sql
CREATE TABLE cobble_sink (
  id BIGINT,
  name STRING,
  score INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 's3://analytics/cobble/users',
  'bucket' = '16',
  'sink.parallelism' = '4',
  's3.endpoint' = 'https://s3.example.com',
  's3.access-key' = '<access-key>',
  's3.secret-key' = '<secret-key>',
  's3.path.style.access' = 'true',
  's3.region' = 'us-east-1'
);
```

Cobble source accepts the same options when it reads this table. The aliases
`s3.access.key` and `s3.secret.key` are also accepted. Explicit access and
secret keys must be supplied together. When a custom endpoint is set without a
region, the region defaults to `us-east-1`.

For another supported filesystem URI, use
`storage.option.<provider-key>`. Explicit table options take precedence; when
they are omitted, Cobble uses the filesystem configuration already available to
the Flink cluster. Replace credential placeholders through deployment templates
or secret management.

## Complete Configuration Reference

This section lists the main configuration keys for the Cobble sink.

### Required Table Requirements

| Item | Requirement |
| --- | --- |
| `connector` | Must be `cobble` |
| `path` | Required |
| `bucket` | Required |
| `sink.parallelism` | Required |
| `PRIMARY KEY` | Required |
| Non-primary-key column | At least one is required |

### Sink Options

| Key | Default | Description |
| --- | --- | --- |
| `path` | none | Cobble table root path or URI. Relative paths are normalized to absolute `file://` URIs. |
| `bucket` | none | Total bucket count used for key routing. |
| `sink.parallelism` | none | Sink writer parallelism. |
| `snapshot.retention` | `1` | Number of committed snapshots to retain automatically. |
| `sink.use-managed-memory-allocator` | `false` | Whether the sink writer declares managed memory usage. |
| `sink.writer-buffer-memory` | `256mb` | Write-buffer budget for one sink writer. |

### Remote storage options

| Key | Default | Description |
| --- | --- | --- |
| `s3.endpoint` | provider default | S3-compatible endpoint. |
| `s3.access-key` | provider chain | Access key; alias: `s3.access.key`. |
| `s3.secret-key` | provider chain | Secret key; alias: `s3.secret.key`. |
| `s3.path.style.access` | provider default | Set `true` for path-style access or `false` for virtual-host-style access. |
| `s3.region` | `us-east-1` with a custom endpoint | S3 region. |
| `storage.option.<provider-key>` | Flink cluster configuration | Filesystem setting for this table root. |

## Metrics

Sink writers register Flink standard send counters and Cobble storage metrics
automatically. See [Cobble Flink Metrics](../metrics/#sink) for row-kind
accounting and byte semantics.

## Usage Notes

- The sink writes by primary key.
- `INSERT` and `UPDATE_AFTER` overwrite the current row for the key.
- `DELETE` removes the current row for the key.
- `UPDATE_BEFORE` is ignored.
- `bucket`, `sink.parallelism`, and `sink.writer-buffer-memory` must all be
  greater than zero.
