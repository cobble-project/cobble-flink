---
title: Home
layout: home
nav_order: 1
---

# Cobble Flink

Cobble Flink integrates [Cobble](https://github.com/cobble-project/cobble) with
[Apache Flink](https://flink.apache.org/) so Cobble can be used as a Flink
state backend, SQL source, and SQL sink.

**Cobble Flink** provides a practical integration layer between Cobble and
Apache Flink. It lets you use Cobble as Flink state storage, read Cobble data
through Flink SQL source tables, and write Flink SQL results back into Cobble.
The default deployment model is simple: most users only need to download the
runtime jar from Maven and place it into the Flink distribution.

Cobble Flink currently targets Flink `1.17.0` and later.

> Note: Cobble Flink is still evolving together with Cobble. We try to keep the
> documentation up to date, but for the latest behavior it is still worth
> checking the code and tests.

## Key Features

- **Cobble state backend** for stateful Flink jobs
- **Cobble SQL source** for reading existing Cobble data in Flink SQL
- **Cobble SQL sink** for writing Flink SQL results into Cobble
- **Shared-storage-friendly state usage** through Cobble's storage model
- **Compatibility with Cobble source capabilities** for later consumption, lookup, and inspection workflows

## Documentation Structure

| Chapter | Description |
| ------- | ----------- |
| [**Introduction**](introduction/) | What Cobble Flink provides and when to use each part |
| [**Getting Started**](getting-started/) | The fastest way to install and configure Cobble Flink |
| [**State Backend**](state-backend/) | How to use Cobble as Flink state storage |
| [**Source**](source/) | How to read data from Cobble in Flink SQL |
| [**Sink**](sink/) | How to write Flink SQL results into Cobble |
