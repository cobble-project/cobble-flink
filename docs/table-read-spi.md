---
title: Read State Outside Flink
nav_exclude: true
---

# Read State Outside Flink

Cobble state snapshots embed a format ID and the schema needed to read their
columns. The existing `cobble-state` jar supplies a Java read plugin for that
format; there is no separate state-read artifact. State key/value bytes are
unchanged.

Applications use `TableReader.open(...)`; Java SPI discovers the format plugin
from the fixed snapshot description before it opens a reader session. Native
tables and state snapshots share the same distributed `TableScanPlan` and split
representation when a caller needs parallel scans.
Install the state jar, its Cobble dependencies, and the matching Flink
runtime dependencies on both the driver and workers. Use the artifact for the
writer's Flink version. Custom serializers may also require the user's jars.

For example, the Cobble Spark connector can read a selected state from a fixed
global snapshot:

```scala
val state = spark.read
  .format("cobble")
  .option("table-name", "word-count")
  .load("file:///data/state-snapshot")

state.show()
```

The path may be a fixed Cobble global snapshot or a completed Flink checkpoint
entry: `_metadata`, `chk-N`, a job directory, or a checkpoint parent. The
resolver pins the selected checkpoint and Cobble operator, validates an existing
matching global manifest when present, and otherwise creates the fixed global
description in memory from checkpoint metadata. Workers open the original shard
manifests directly; state files remain in the original storage and the checkpoint
is never modified. Scope a path to one job directory before using `flink.checkpoint-id`;
that option pins a checkpoint but does not choose between job trees.
`flink.operator-id` selects an operator within the pinned checkpoint. A direct
`SNAPSHOT-N` shard manifest is supported only when it can be uniquely associated
with a completed `_metadata` record. Spark currently supports local paths; the
same path must be accessible to all workers.

Value, reducing, aggregating and map state support exact lookup with the full
key field order reported by `TableReader.keyFields()`. List entries produce one
row per element and support scans only; list and timer lookups are rejected.
All external state readers are read-only.

Snapshots without the embedded format metadata, unknown formats and unsupported
schemas fail explicitly. This feature does not infer a format for older data.
