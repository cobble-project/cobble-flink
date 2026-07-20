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
running job and restore tooling. Cobble Flink makes persisted state easier to
understand and reuse:

- **See what Flink stored.** The web monitor can browse checkpoints and sink
  snapshots by operator and state, then decode keys and values into semantic
  fields when schema information is available.
- **Consume state as data.** The SQL source can scan or look up Cobble sink
  tables and supported keyed state directly, including structured semantic
  columns. Persisted state is no longer useful only for job recovery.
- **Use one storage layer across workflows.** State backend, source, sink,
  metrics, and remote storage support work together, making it easier to debug
  a job, validate its state, and build new Flink pipelines from existing data.

This brings a different experience to stateful Flink: managed state remains
part of the runtime while becoming observable and consumable. The same applies
to tables written by the Cobble sink: users can inspect their snapshots and
read the data back through the Cobble source.

## Showcase

The diagram shows how Cobble connects Flink storage and consumption paths:

- Flink jobs can persist managed state or sink tables in Cobble while
  processing streams such as Kafka topics.
- Other Flink jobs can scan or continuously read the persisted data, or use it
  for exact-key lookup joins.
- The web monitor reads the same snapshots so users can inspect keys, values,
  and semantic columns without modifying the running job.

![Cobble Flink state, source, sink, lookup, and web monitor workflows](assets/images/cobble-flink-showcase.jpg)

## Documentation Structure

| Chapter | Description |
| --- | --- |
| [Getting Started](getting-started/) | Install Cobble Flink and choose a version |
| [State Backend](state-backend/) | Store Flink managed state in Cobble |
| [Source](source/) | Read Cobble data from Flink SQL |
| [Sink](sink/) | Write Flink SQL results to Cobble |
| [Web Monitor](web-monitor/) | Inspect checkpoints and tables |
| [Metrics](metrics/) | Monitor state backend and connector activity |
