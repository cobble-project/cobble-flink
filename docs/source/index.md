---
title: Source
nav_order: 6
---

# Source

Use the Cobble source when you want to read existing Cobble data from Flink
SQL. The source can read Cobble sink table snapshots, and it can also read
Flink keyed state written by the Cobble state backend.

## Why Use Cobble Source

Cobble source is useful when your data is already stored in Cobble and you want
to consume it directly inside Flink.

- **Read Cobble data directly in Flink SQL**
  You can define a Flink table on top of existing Cobble data or a Cobble state
  checkpoint and query it without adding a separate import step.
- **Support both batch-style reading and continuous follow-up**
  You can read the latest snapshot once, or keep following new snapshots in
  streaming mode.
- **Convenient for lookup use cases**
  Sink tables and supported keyed states can be used as lookup tables for
  dimension enrichment in Flink SQL.

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

### Choose the source kind

The same connector name is used for both source kinds:

- `connector = 'cobble'`
- a required `path`
- `source.kind = 'sink'`, `source.kind = 'state'`, or the default
  `source.kind = 'auto'`

Use `sink` when the path is a Cobble table written by the Cobble SQL sink. Use
`state` when the path is a Flink checkpoint root or a concrete `chk-*`
directory written with Cobble as the state backend. `auto` works for the common
on-disk layouts, but setting the kind explicitly makes production DDL easier to
read.

## Semantic Columns

When Cobble data is produced by Cobble Flink, the writer can persist inspect
schema metadata beside the data. The source reads that metadata during planning
and exposes typed SQL columns instead of asking you to work with raw key/value
bytes.

For a Cobble sink table, semantic columns are the table columns from the sink
DDL: primary-key columns first according to the declared primary key, and value
columns with their original names and logical types.

For Cobble state written by a Flink SQL job, semantic columns come from the SQL
operator state when Flink still has enough type information. Common examples
include join state and some deduplicate or sort state. If Flink has already
discarded the original SQL aliases, Cobble still exposes typed fallback columns
such as `f0`, `f1`, and `value`.

For MapState, the semantic table shape is:

- state key columns;
- namespace columns when the state uses a non-void namespace;
- map key columns;
- map value columns.

For example, a SQL join operator may store rows by `customer_id` and keep the
right-side records in a map keyed by `order_id`. The Cobble source can then
expose a table like:

```sql
CREATE TABLE order_join_state (
  customer_id BIGINT,
  order_id BIGINT,
  amount DECIMAL(12, 2),
  status STRING,
  PRIMARY KEY (customer_id, order_id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'state',
  'path' = 'file:///tmp/flink-checkpoints',
  'state.operator-id' = '<operator-id>',
  'state.name' = 'right-records',
  'state.kind' = 'map',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

The primary key contains the full MapState entry key: `customer_id` from the
state key and `order_id` from the map key. `amount` and `status` are map value
columns, so they are not part of the primary key.

## Read a Cobble Sink Table

```sql
CREATE TABLE cobble_source (
  id BIGINT,
  name STRING,
  score INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'sink',
  'path' = 'file:///tmp/cobble-table',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

Sink tables require a `PRIMARY KEY` and at least one value column. The same table
can be used as a lookup dimension table:

```sql
SELECT o.order_id, o.id, d.name, d.score
FROM orders AS o
LEFT JOIN cobble_source FOR SYSTEM_TIME AS OF o.pt AS d
ON o.id = d.id;
```

## Read Cobble State

State sources read keyed state from a Flink checkpoint. The `path` points to the
checkpoint root or a concrete `chk-*` directory. If the checkpoint has several
Cobble operators, set `state.operator-id`; otherwise it can be omitted.

### Scan ValueState

Scanning does not require a primary key. The table columns must match the state
schema written by the Cobble state backend.

```sql
CREATE TABLE state_values (
  `key` INT,
  `value` INT
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'state',
  'path' = 'file:///tmp/flink-checkpoints',
  'state.operator-id' = '<operator-id>',
  'state.name' = 'value-state',
  'state.kind' = 'value',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);
```

### Lookup ValueState

State lookup is exact-key lookup. Declare a `PRIMARY KEY` that contains the full
state key. Use a temporal lookup join with a processing-time column on the probe
side:

```sql
CREATE TABLE state_values (
  `key` INT,
  `value` INT,
  PRIMARY KEY (`key`) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'state',
  'path' = 'file:///tmp/flink-checkpoints',
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

### Lookup MapState

MapState lookup requires the full map-entry key: state key, namespace when the
state uses one, and map key. The map value is not part of the primary key.

```sql
CREATE TABLE order_join_state (
  customer_id BIGINT,
  order_id BIGINT,
  amount DECIMAL(12, 2),
  status STRING,
  PRIMARY KEY (customer_id, order_id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'state',
  'path' = 'file:///tmp/flink-checkpoints',
  'state.operator-id' = '<operator-id>',
  'state.name' = 'right-records',
  'state.kind' = 'map',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);

SELECT p.customer_id, p.order_id, d.amount, d.status
FROM probes AS p
LEFT JOIN order_join_state FOR SYSTEM_TIME AS OF p.pt AS d
ON p.customer_id = d.customer_id
AND p.order_id = d.order_id;
```

For non-void namespaces, include the namespace columns before the map key:

```sql
PRIMARY KEY (customer_id, namespace, order_id) NOT ENFORCED
```

## State Source Support Matrix

| State kind | Scan | Lookup | Lookup key |
| --- | --- | --- | --- |
| `ValueState` | supported | supported | state key, plus namespace when present |
| `ReducingState` | supported | supported | state key, plus namespace when present |
| `AggregatingState` | supported | supported | state key, plus namespace when present |
| `MapState` | supported | supported | state key, plus namespace when present, plus map key |
| `ListState` | supported | not supported | none |
| Timer state | supported | not supported | none |

## Complete Configuration Reference

This section lists the main configuration keys for the Cobble source.

### Sink Table Requirements

| Item | Requirement |
| --- | --- |
| `connector` | Must be `cobble` |
| `path` | Required |
| `source.kind` | Use `sink` or leave as `auto` |
| `PRIMARY KEY` | Required for sink tables |
| Non-primary-key column | At least one is required |

### State Table Requirements

| Item | Requirement |
| --- | --- |
| `connector` | Must be `cobble` |
| `path` | Checkpoint root URI or a concrete `chk-*` URI |
| `source.kind` | Use `state` or leave as `auto` when the path layout is unambiguous |
| `state.name` | Required |
| `state.operator-id` | Optional only when the checkpoint has exactly one Cobble operator |
| `state.kind` | Optional validation hint: `value`, `list`, `map`, `reducing`, `aggregating`, or `timer` |
| `PRIMARY KEY` | Required only for lookup joins |

### Source Options

| Key | Default | Description |
| --- | --- | --- |
| `path` | none | Cobble table root path, checkpoint root path, or concrete `chk-*` path. Relative paths are normalized to absolute `file://` URIs. |
| `source.kind` | `auto` | Source kind: `auto`, `sink`, or `state`. |
| `bucket` | inferred from snapshot metadata | Total bucket count or key-group count. Usually optional. |
| `scan.checkpoint-id` | `latest` | Snapshot to read. Use `latest` or a positive numeric checkpoint id. |
| `scan.mode` | `batch` | Read mode. Supported values are `batch` and `streaming`. |
| `scan.poll-interval-ms` | `3000` | Poll interval used by `latest` + `streaming`. |
| `source.block-cache-memory` | `0` | Block-cache memory mainly used by lookup access. |
| `state.name` | none | State name to read when `source.kind = 'state'`. |
| `state.operator-id` | inferred when possible | Cobble operator id to read when `source.kind = 'state'`. |
| `state.kind` | inferred from metadata | Optional state-kind validation hint. |

## Usage Notes

- `scan.mode = 'streaming'` currently works only with
  `scan.checkpoint-id = 'latest'`.
- The source currently returns `INSERT` rows only.
- If `bucket` is specified, it must be greater than zero.
- `scan.poll-interval-ms` must be greater than zero.
- Lookup joins require equality conditions on the full `PRIMARY KEY`.
- Lookup joins currently support only top-level key columns.
- State lookup currently supports only `scan.mode = 'batch'`.
- State lookup is exact-key lookup only. Prefix lookup and partial-key lookup
  are not supported.
