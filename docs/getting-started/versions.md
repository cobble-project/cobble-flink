---
title: Versions
parent: Getting Started
nav_order: 1
---

# Versions

Cobble Flink artifacts should be selected with the correct version naming rule:

```text
${cobble-version}-{patch-version}-flink-{flink-minor-version}
```

Example:

```text
0.5.2-1-flink-1.17
```

Use the matrix below to choose the `<version>` value for each dependency in
your `pom.xml` or for the runtime jar you copy into Flink `lib/`:

| Flink cluster version | Dist bundle jar version | State dependency version | Sink dependency version | Source dependency version |
| --- | --- | --- | --- | --- |
| 1.17, 1.18 | `0.5.2-1-flink-1.17` | `0.5.2-1-flink-1.17` | `0.5.2-1-flink-1.17` | `0.5.2-1-flink-1.17` |
| 1.19, 1.20 | `0.5.2-1-flink-1.19` | `0.5.2-1-flink-1.19` | `0.5.2-1-flink-1.17` | `0.5.2-1-flink-1.17` |
| 2.0 | `0.5.2-1-flink-2.0` | `0.5.2-1-flink-2.0` | `0.5.2-1-flink-2.0` | `0.5.2-1-flink-1.17` |
| 2.1 and above | `0.5.2-1-flink-2.1` | `0.5.2-1-flink-2.1` | `0.5.2-1-flink-2.0` | `0.5.2-1-flink-1.17` |

The `0.5.2-1` release line uses Cobble core `0.5.2`. Use the bundled
`cobble-flink-dist` jar for a complete installation. For job-side dependencies,
use the state, sink, or source column with the corresponding artifact below.
All artifacts use groupId `io.github.cobble-project`.

| Artifact | Maven Central downloads |
| --- | --- |
| `cobble-flink-dist` | [Runtime bundle](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-dist/) |
| `cobble-flink-state` | [State backend](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-state/) |
| `cobble-flink-sink` | [SQL sink](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-sink/) |
| `cobble-flink-source` | [SQL source](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-source/) |

## Monitor Versions

The standalone monitor variants follow the same `0.5.2-1` release line:

| Checkpoint producer line | Bundled Flink runtime | Monitor version |
| --- | --- | --- |
| 1.17 | 1.17.2 | `0.5.2-1-flink-1.17` |
| 1.19 | 1.19.3 | `0.5.2-1-flink-1.19` |
| 2.0 | 2.0.2 | `0.5.2-1-flink-2.0` |
| 2.1 and above | 2.1.1 | `0.5.2-1-flink-2.1` |

Download the runnable jar from [Maven Central](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-monitor/).

All use artifactId `cobble-flink-monitor` and the same source code and web UI.
Match the monitor to the checkpoint producer line; the 1.17 runtime cannot
read all newer `_metadata` formats. Flink 2.1 and above use the
`flink-2.1` monitor artifact. The bundled runtime column identifies the
Flink dependency packaged in the monitor, not an upper compatibility limit.
See [Web Monitor](../web-monitor/) for usage.

