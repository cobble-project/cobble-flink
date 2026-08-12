---
title: Introduction
layout: home
nav_order: 1
---

# Introduction

Cobble Flink integrates [Cobble](https://github.com/cobble-project/cobble) with
[Apache Flink®](https://flink.apache.org/), so you can use Cobble as a Flink
state backend, SQL source, and SQL sink.

## Features

Cobble Flink currently provides:

- a **state backend** for stateful Flink jobs
- a **SQL source** for reading Cobble data in Flink SQL
- a **SQL sink** for writing Flink SQL results into Cobble
- a bundled **runtime jar** for Flink cluster deployment
- a **web monitor** for inspecting checkpoint and sink snapshots

Cobble Flink supports Flink `1.17` and later. See
[Getting Started](getting-started/) for the version matrix.

## Why Cobble Flink

Flink state is essential to a stateful job, but it is often visible only to the
running job and restore tooling. Cobble Flink provides a state backend built for
performance, elastic storage, and a more open state experience:

- **High performance for stateful workloads.** Cobble combines adaptive
  in-memory structures, efficient state operations, and an LSM engine designed
  for streaming workloads. On Flink 2.0, it also supports the asynchronous state
  API introduced by Flink. See the current
  [state backend benchmarks](https://cobble-project.github.io/cobble-flink/latest/state-backend/benchmark)
  for Nexmark and Flink state-operation results.
- **Storage-compute separation with a local fast path.** Durable state can live
  on shared or object storage for recovery and rescale, while the local volume
  remains the high-priority active tier by default. Normal processing therefore
  uses local storage first without giving up shared-state elasticity.
- **Key-value separation for large state values.** Values above a configurable
  threshold are stored in Cobble's value log instead of being repeatedly
  rewritten with SST keys and indexes. Value-log files can also be assigned to
  a lower-priority primary tier when required by the deployment.
- **Observe persisted state.** The web monitor can browse checkpoint and sink
  snapshots by operator and state, decode keys and values into semantic fields,
  and surface generated SQL examples.
- **Consume state as data.** The SQL source can scan or look up Cobble sink
  tables and supported keyed state directly, including structured semantic
  columns. Persisted state is no longer useful only for job recovery.

State backend, source, sink, metrics, and remote storage support work together
as one storage layer. Managed state remains part of the Flink runtime while
becoming observable and consumable; tables written by the Cobble sink gain the
same inspection and source capabilities.

## Showcase

The diagram shows how Cobble connects Flink storage and consumption paths:

- Flink jobs can persist managed state or sink tables in Cobble while
  processing streams such as Kafka topics.
- Other Flink jobs can scan or continuously read the persisted data, or use it
  for exact-key lookup joins.
- The web monitor reads the same snapshots so users can inspect keys, values,
  and semantic columns without modifying the running job.

<p align="center"><img src="assets/images/cobble-flink-showcase.jpg" style="max-width: 60%; height: auto;" alt="Cobble Flink state, source, sink, lookup, and web monitor workflows" /></p>

## Documentation Structure

| Chapter | Description |
| --- | --- |
| [Getting Started](getting-started/) | Install Cobble Flink and choose a version |
| [State Backend](state-backend/) | Store Flink managed state in Cobble |
| [Source](source/) | Read Cobble data from Flink SQL |
| [Sink](sink/) | Write Flink SQL results to Cobble |
| [Web Monitor](web-monitor/) | Inspect checkpoints and tables |
| [Metrics](metrics/) | Monitor state backend and connector activity |
