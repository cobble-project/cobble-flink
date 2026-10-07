---
title: Introduction
layout: home
nav_order: 1
---

# Introduction

<p align="center"><img src="{{ '/assets/images/logos/cobble-horizontal-1024.png' | relative_url }}" style="max-width: 60%; height: auto;" alt="Cobble Project logo" /></p>

**Cobble Flink** is unified storage for Apache Flink: write processing results,
read stored data, and manage streaming state through the same
[Cobble core](https://github.com/cobble-project/cobble).

## Watch the Introduction

<video controls preload="metadata" playsinline style="width: 100%; height: auto;" poster="{{ '/assets/videos/cobble-flink-intro-cover.jpg' | relative_url }}">
  <source src="{{ '/assets/videos/cobble-flink-intro-en.mp4' | relative_url }}" type="video/mp4">
  <a href="{{ '/assets/videos/cobble-flink-intro-en.mp4' | relative_url }}">Download the introduction video</a>.
</video>

[Watch or download the introduction (2 min 44 sec)]({{ '/assets/videos/cobble-flink-intro-en.mp4' | relative_url }}).

## Use Cases

### Balance storage cost and state performance

Separate storage from compute, starting with Flink 1.17. Choose hybrid storage
or local caching to balance capacity, cost, and performance.

<p align="center">
  <img src="{{ '/assets/images/use-cases/flexible-storage.svg' | relative_url }}" width="100%" alt="Cobble core manages local and remote storage with hybrid or local-cache policies." />
</p>

[Explore the state backend](state-backend/)
· [See benchmarks](state-backend/benchmark)

### Share compaction capacity across jobs

Offload compaction from processing jobs and share CPU capacity across their peaks.

<p align="center">
  <img src="{{ '/assets/images/use-cases/remote-compaction.svg' | relative_url }}" width="100%" alt="Illustrative remote compaction sizing: six provisioned CPUs become four with staggered peaks." />
</p>

[Configure remote compaction](state-backend/#remote-compaction)

### Write once, reuse across jobs

Let another job scan stored results or look up a key for enrichment. Supported
keyed state is reusable too.

<p align="center">
  <img src="{{ '/assets/images/use-cases/data-reuse.svg' | relative_url }}" width="100%" alt="A Flink writer publishes Cobble snapshots for other jobs to scan or look up by key." />
</p>

[Write with the sink](sink/)
· [Scan and look up data](source/)

### See what changed in your state

Look up state and sink records, inspect business fields, and track changes
across snapshots to debug your job.

<p align="center">
  <img src="{{ '/assets/images/use-cases/state-inspection.svg' | relative_url }}" width="100%" alt="Web Monitor illustration showing a customer record changing between completed checkpoints." />
</p>

[Explore the Web Monitor](web-monitor/)

## Documentation Structure

| Chapter | Description |
| --- | --- |
| [Getting Started](getting-started/) | Install Cobble Flink and choose a version |
| [State Backend](state-backend/) | Store Flink managed state in Cobble |
| [Catalog](catalog/) | Create and use native Cobble SQL tables |
| [Source](source/) | Read Cobble data from Flink SQL |
| [Sink](sink/) | Write Flink SQL results to Cobble |
| [Web Monitor](web-monitor/) | Inspect checkpoints and tables |
| [Metrics](metrics/) | Monitor state backend and connector activity |
