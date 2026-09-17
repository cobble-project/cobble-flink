---
title: Catalog
nav_order: 4
---

# Catalog

The Cobble catalog lets Flink SQL create, evolve, write, and read typed Cobble
tables without repeating connector paths in every table definition. A catalog
table uses the same native schema for `INSERT`, batch scans, and exact-key
lookups. See [Source](../source/) for scan and lookup join details.

## Create a Catalog

```sql
CREATE CATALOG lake WITH (
  'type' = 'cobble',
  'path' = 'file:///data/cobble/warehouse',
  'storage-id' = 'production',
  'buckets' = '16'
);
USE CATALOG lake;
CREATE DATABASE analytics;
USE analytics;
```

`path` is the warehouse location. `storage-id` separates catalog metadata
stored at the same location and defaults to `flink`. `buckets` is the fixed
bucket count for catalog tables and must be between 1 and 65536.

For object storage, use the same storage settings accepted by the source and
sink, such as `storage.option.endpoint`, `storage.option.region`,
`s3.access-key`, and `s3.secret-key`. They are carried into the job so workers
reopen the same storage location.

## Create and Use a Table

```sql
CREATE TABLE orders (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  amount DECIMAL(18, 2),
  PRIMARY KEY (order_id, customer_id) NOT ENFORCED
);

INSERT INTO orders VALUES (42, 7, DECIMAL '19.95');

SELECT * FROM orders;

SELECT * FROM orders
WHERE order_id = 42 AND customer_id = 7;
```

Catalog sinks synchronously upload one snapshot per owned bucket at each
checkpoint. A catalog sink defaults to one writer unless `sink.parallelism` is
supplied. Restoring with the same writer parallelism is supported; changing
writer parallelism is rejected. Catalog reads are bounded
snapshot scans, not a continuous-change feed.

`snapshot.retention` expires older global snapshots. Catalog shard snapshots
that are no longer referenced are retained until native catalog shard garbage
collection is available.

## Schema and ALTER

Supported column types are `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`,
`FLOAT`, `DOUBLE`, unbounded `STRING`/`VARCHAR`, unbounded
`BYTES`/`VARBINARY`, `DECIMAL`, `DATE`, `TIME`, and `TIMESTAMP`. Every table
needs a primary key and at least one non-key column.

Supported operations are:

- append a nullable physical column;
- drop a non-key column;
- rename a column;
- widen a non-key integer or floating-point type, or increase compatible
  `DECIMAL`, `TIME`, or `TIMESTAMP` precision.

Computed columns, watermarks, partitions, comments, table options, primary-key
changes, and other ALTER operations are rejected. Bounded `VARCHAR` and
`VARBINARY` are also rejected because Cobble cannot preserve those bounds.

An ALTER changes the catalog schema, not already committed shard snapshots.
Write and commit at least one refreshed snapshot after an ALTER before scanning
the table; scans of an older incompatible snapshot are rejected rather than
silently reshaping rows.
