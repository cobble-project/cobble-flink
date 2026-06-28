---
title: Source
nav_order: 6
---

# Source

Use the Cobble source when you want to read existing Cobble data from Flink
SQL.

## Why Use Cobble Source

Cobble source is useful when your data is already stored in Cobble and you want
to consume it directly inside Flink.

- **Read Cobble data directly in Flink SQL**
  You can define a Flink table on top of existing Cobble data and query it
  without adding a separate import step.
- **Support both batch-style reading and continuous follow-up**
  You can read the latest snapshot once, or keep following new snapshots in
  streaming mode.
- **Convenient for lookup use cases**
  The same table can also be used as a lookup table for dimension enrichment in
  Flink SQL.

## Installation and Setup

For most users, the easiest setup is to download the runtime jar into the Flink
distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download the released runtime jar from a Maven repository and put it into
Flink's `lib/` directory. Use the version matrix in
[Getting Started](../getting-started/) to choose the artifact that matches your
Flink version. For example, on Flink 1.17 or later:

```bash
cp cobble-flink-dist-0.2.0-1-flink-1.17.jar "$FLINK_HOME/lib/"
```

### Option B: use a job-side dependency

Add `cobble-flink-source` to your job if you prefer to package the connector
with the job itself:

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-source</artifactId>
  <version>${cobble.flink.version}</version>
</dependency>
```

Most users do not need this if the runtime jar is already present in Flink
`lib/`. `cobble-flink-source` is currently the source dependency for all
supported Flink versions; the matching runtime jar is still selected from the
matrix in [Getting Started](../getting-started/).

### Define the source table

The Cobble source uses:

- `connector = 'cobble'`
- a required `path`
- a required `PRIMARY KEY`
- at least one non-primary-key column

## Minimal SQL Example

```sql
CREATE TABLE cobble_source (
  id BIGINT,
  name STRING,
  score INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'file:///tmp/cobble-table',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

## Lookup Join Example

The same table can be used as a lookup dimension table:

```sql
SELECT o.order_id, o.id, d.name, d.score
FROM orders AS o
LEFT JOIN cobble_source FOR SYSTEM_TIME AS OF o.pt AS d
ON o.id = d.id;
```

## Complete Configuration Reference

This section lists the main configuration keys for the Cobble source.

### Required Table Requirements

| Item | Requirement |
| --- | --- |
| `connector` | Must be `cobble` |
| `path` | Required |
| `PRIMARY KEY` | Required |
| Non-primary-key column | At least one is required |

### Source Options

| Key | Default | Description |
| --- | --- | --- |
| `path` | none | Cobble table root path or URI. Relative paths are normalized to absolute `file://` URIs. |
| `bucket` | inferred from snapshot metadata | Total bucket count of the Cobble table. Usually optional. |
| `scan.checkpoint-id` | `latest` | Snapshot to read. Use `latest` or a positive numeric checkpoint id. |
| `scan.mode` | `batch` | Read mode. Supported values are `batch` and `streaming`. |
| `scan.poll-interval-ms` | `3000` | Poll interval used by `latest` + `streaming`. |
| `source.block-cache-memory` | `0` | Block-cache memory mainly used by lookup access. |

## Usage Notes

- `scan.mode = 'streaming'` currently works only with
  `scan.checkpoint-id = 'latest'`.
- The source currently returns `INSERT` rows only.
- If `bucket` is specified, it must be greater than zero.
- `scan.poll-interval-ms` must be greater than zero.
- Lookup joins require equality conditions on the full `PRIMARY KEY`.
- Lookup joins currently support only top-level key columns.
