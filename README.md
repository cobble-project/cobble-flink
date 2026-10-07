<p align="center"><img src="https://github.com/cobble-project/cobble-flink/raw/HEAD/docs/assets/images/logos/cobble-horizontal-1024.png" width="60%" alt="Cobble Project logo" /></p>
<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.cobble-project/cobble-flink-state"><img alt="Maven Central" src="https://img.shields.io/maven-central/v/io.github.cobble-project/cobble-flink-state?logo=apachemaven" /></a>
  <a href="https://github.com/cobble-project/cobble-flink/blob/main/LICENSE"><img alt="GitHub License" src="https://img.shields.io/github/license/cobble-project/cobble-flink" /></a>
  <a href="https://cobble-project.github.io/cobble-flink/latest/"><img alt="Documentation" src="https://img.shields.io/badge/docs-GitHub%20Pages-222222?logo=githubpages" /></a>
  <a href="https://github.com/cobble-project/cobble-flink/actions/workflows/ci.yml"><img alt="GitHub CI" src="https://img.shields.io/github/actions/workflow/status/cobble-project/cobble-flink/ci.yml?label=CI&logo=githubactions" /></a>
  <a href="https://cobble-project.github.io/cobble-flink/latest/getting-started/"><img alt="Flink compatibility" src="https://img.shields.io/badge/Flink-1.17%20to%202.x-E6526F?logo=apacheflink" /></a>
</p>

**Cobble Flink** is unified storage for Apache Flink: write processing results,
read stored data, and manage streaming state through the same
[Cobble core](https://github.com/cobble-project/cobble).

https://github.com/user-attachments/assets/5b9406ff-4d18-4247-899d-6d10d8685c70

*Cobble Flink: flexible storage, reusable data, and visible state.*

[Documentation](https://cobble-project.github.io/cobble-flink/latest/)
· [Getting started](https://cobble-project.github.io/cobble-flink/latest/getting-started/)

## Use Cases

### Balance storage cost and state performance

Separate storage from compute, starting with Flink 1.17. Choose hybrid storage
or local caching to balance capacity, cost, and performance.

<p align="center">
  <img src="docs/assets/images/use-cases/flexible-storage.svg" width="100%" alt="Cobble core manages local and remote storage with hybrid or local-cache policies." />
</p>

[Explore the state backend](https://cobble-project.github.io/cobble-flink/latest/state-backend/)
· [See benchmarks](https://cobble-project.github.io/cobble-flink/latest/state-backend/benchmark)

### Share compaction capacity across jobs

Offload compaction from processing jobs and share CPU capacity across their peaks.

<p align="center">
  <img src="docs/assets/images/use-cases/remote-compaction.svg" width="100%" alt="Illustrative remote compaction sizing: six provisioned CPUs become four with staggered peaks." />
</p>

[Configure remote compaction](https://cobble-project.github.io/cobble-flink/latest/state-backend/#remote-compaction)

### Write once, reuse across jobs

Let another job scan stored results or look up a key for enrichment. Supported
keyed state is reusable too.

<p align="center">
  <img src="docs/assets/images/use-cases/data-reuse.svg" width="100%" alt="A Flink writer publishes Cobble snapshots for other jobs to scan or look up by key." />
</p>

[Write with the sink](https://cobble-project.github.io/cobble-flink/latest/sink/)
· [Scan and look up data](https://cobble-project.github.io/cobble-flink/latest/source/)

### See what changed in your state

Look up state and sink records, inspect business fields, and track changes
across snapshots to debug your job.

<p align="center">
  <img src="docs/assets/images/use-cases/state-inspection.svg" width="100%" alt="Web Monitor illustration showing a customer record changing between completed checkpoints." />
</p>

[Explore the Web Monitor](https://cobble-project.github.io/cobble-flink/latest/web-monitor/)

## Getting Started

Follow the [Getting Started guide](https://cobble-project.github.io/cobble-flink/latest/getting-started/)
for version selection, installation, state-backend configuration, and
SQL examples for writes, scans, and lookup joins.

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Notice

The [Apache Flink®](https://flink.apache.org/) is registered trademarks of The Apache Software Foundation in the United States and other countries.
