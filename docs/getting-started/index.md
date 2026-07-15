---
title: Getting Started
nav_order: 3
---

# Getting Started

This page gives the shortest path to start using Cobble Flink.

## Version Selection

Cobble Flink artifacts should be selected with the correct version naming rule:

```text
${cobble-version}-{patch-version}-flink-{flink-minor-version}
```

Example:

```text
0.2.2-1-flink-1.17
```

Use the matrix below to choose the `<version>` value for each dependency in
your `pom.xml` or for the runtime jar you copy into Flink `lib/`:

| Flink cluster version | Dist bundle jar version | State dependency version | Sink dependency version | Source dependency version |
| --- | --- | --- | --- | --- |
| 1.17, 1.18 | `0.2.2-1-flink-1.17` | `0.2.2-1-flink-1.17` | `0.2.2-1-flink-1.17` | `0.2.2-1-flink-1.17` |
| 1.19, 1.20 | `0.2.2-1-flink-1.19` | `0.2.2-1-flink-1.19` | `0.2.2-1-flink-1.17` | `0.2.2-1-flink-1.17` |
| 2.0 and above | `0.2.2-1-flink-2.0` | `0.2.2-1-flink-2.0` | `0.2.2-1-flink-2.0` | `0.2.2-1-flink-1.17` |

The dist bundle jar is a single artifact that contains all three parts, it is the recommended way to use Cobble Flink.
The other three parts are separate artifacts that can be used as job-side Maven dependencies.
ArtifactIds stay the same across Flink versions. Choose the artifactId from
the section you are using, then choose the `<version>` from the table.

Make sure the version matches:

- the Cobble version you want to use
- the Flink minor version of your cluster
- the artifact form you choose to use

## Flink Cluster Configuration

Use the configuration file and checkpoint directory key for your Flink version:

| Flink version | Cluster configuration file | Primary checkpoint directory key |
| --- | --- | --- |
| 1.17, 1.18 | `$FLINK_HOME/conf/flink-conf.yaml` | `state.checkpoints.dir` |
| 1.19 | `$FLINK_HOME/conf/config.yaml` by default; legacy `flink-conf.yaml` is supported and wins when both files exist | `state.checkpoints.dir` |
| 1.20 | `$FLINK_HOME/conf/config.yaml` by default; legacy `flink-conf.yaml` is supported and wins when both files exist | `execution.checkpointing.dir` (`state.checkpoints.dir` is a deprecated alias) |
| 2.0 and above | `$FLINK_HOME/conf/config.yaml` only | `execution.checkpointing.dir` (`state.checkpoints.dir` is a deprecated alias) |

`state.backend.type`, `high-availability.type`, `env.java.opts.all`, and
`env.java.home` use the same keys in every supported version. Only the
checkpoint directory key changes at Flink 1.20.

## Setup

For most users, the recommended setup is to download the runtime jar into the
Flink distribution.

Job-side Maven dependency is an alternative packaging choice. You usually do
not need both at the same time.

### Option A: use the runtime jar (recommended)

Download the released runtime jar artifact from the table above and place it in
the Flink distribution's `lib/` directory. For example, on Flink 1.17 or 1.18:

```bash
export FLINK_HOME=/path/to/flink-1.17.x
cp cobble-flink-dist-0.2.2-1-flink-1.17.jar "$FLINK_HOME/lib/"
```

If you are not using a released jar yet and want to build from source, you can
build the distribution jar locally:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl :cobble-flink-dist -am package -DskipTests

cp cobble-dist/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 1.19 or 1.20, build `cobble-dist-flink-1.19`:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl cobble-common,cobble-state-flink-1.19,cobble-sink,cobble-source,cobble-dist-flink-1.19 \
  package -DskipTests

cp cobble-dist-flink-1.19/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For Flink 2.0 and above, build `cobble-dist-flink-2.0`:

```bash
./mvnw --batch-mode --no-transfer-progress \
  -pl cobble-common,cobble-state-flink-2.0,cobble-sink-flink-2.0,cobble-source,cobble-dist-flink-2.0 \
  package -DskipTests

cp cobble-dist-flink-2.0/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

These runtime jar artifacts can be built in the same reactor; no Maven profile
switch is required.

For normal users, the important point is simple: the Flink cluster should use
the bundled `dist` jar.

### Option B: use job-side Maven dependencies

For your own job, add the dependencies you actually use instead of the bundled
`dist` jar.

The following example is for a Flink 1.19 or 1.20 job that uses all three
parts. The state backend uses the 1.19-compatible artifact version, while sink
and source can use the 1.17-compatible artifact version:

```xml
<dependencies>
  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-state</artifactId>
    <version>0.2.2-1-flink-1.19</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-source</artifactId>
    <version>0.2.2-1-flink-1.17</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-sink</artifactId>
    <version>0.2.2-1-flink-1.17</version>
  </dependency>
</dependencies>
```

Common choices:

- stateful DataStream job: `cobble-flink-state`
- SQL read job: `cobble-flink-source`
- SQL write job: `cobble-flink-sink`
- mixed usage: add multiple dependencies

### Configure Flink if you use the state backend

If you want to use Cobble as the Flink state backend, add these shared settings
to the [cluster configuration file](#flink-cluster-configuration) for your
Flink version:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
state.backend.cobble.localdir: /tmp/flink-cobble/local
state.backend.cobble.memory.managed: true

# Recommended, optional: materialize checkpoint sidecars for faster monitor/source reads.
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
```

Then choose one primary checkpoint directory setting from the table above:

```yaml
# Flink 1.17, 1.18, or 1.19
state.checkpoints.dir: hdfs:///user/you/checkpoints
```

For Flink 1.20 or 2.0+, use this instead:

```yaml
execution.checkpointing.dir: hdfs:///user/you/checkpoints
```

The Cobble HA wrapper is recommended when you want sidecars materialized as checkpoints
complete, but it is not required for monitor or source access. When it is not enabled, the monitor and
state source can read Cobble payloads from Flink `_metadata` and rebuild a temporary read-only
view. To enable the wrapper, set `high-availability.type` to
`io.cobble.flink.state.CobbleHighAvailabilityServicesFactory`; move an existing HA type to
`cobble.ha.delegate.type`.

Flink 2.0 deployments normally use Java 17. This does not imply that Java 11
cannot be used where the Flink distribution supports it.

Flink 1.19 and newer distributions normally already set `env.java.opts.all`
with the required `sun.nio.ch`, `java.lang`, and `java.util` access flags. Do
not replace that value with a shorter Cobble-specific value: preserve it and
append only flags that are missing. In the usual 1.19+ distribution setup, no
change is needed.

For Flink 1.17 or 1.18, add this single-line setting if those flags are not
already present:

```yaml
env.java.opts.all: "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
```

If multiple JDKs are installed, pin the runtime explicitly if needed:

```yaml
env.java.home: /path/to/your/jdk
```

If your job only uses source or sink, you do not need this state-backend
configuration.

### Start Flink and submit the job

1. If you chose the runtime-jar setup, put the Cobble jar into `$FLINK_HOME/lib`
2. Update the cluster configuration file for your Flink version if you use the state backend
3. Start the cluster with `$FLINK_HOME/bin/start-cluster.sh`
4. Submit your job with `$FLINK_HOME/bin/flink run ...`

## Where To Go Next

- [State Backend](../state-backend/) if your job is stateful
- [Source](../source/) if you want to read from Cobble
- [Sink](../sink/) if you want to write into Cobble
