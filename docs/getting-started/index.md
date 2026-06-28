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
0.2.0-1-flink-1.17
```

Use the matrix below to choose the `<version>` value for each dependency in
your `pom.xml` or for the runtime jar you copy into Flink `lib/`:

| Flink cluster version | Dist bundle jar version | State dependency version | Sink dependency version | Source dependency version |
| --- | --- | --- | --- | --- |
| 1.17, 1.18 | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` |
| 1.19, 1.20 | `0.2.0-1-flink-1.19` | `0.2.0-1-flink-1.19` | `0.2.0-1-flink-1.17` | `0.2.0-1-flink-1.17` |
| 2.0 and above | `0.2.0-1-flink-2.0` | `0.2.0-1-flink-2.0` | `0.2.0-1-flink-2.0` | `0.2.0-1-flink-1.17` |

The dist bundle jar is a single artifact that contains all three parts, it is the recommended way to use Cobble Flink.
The other three parts are separate artifacts that can be used as job-side Maven dependencies.
ArtifactIds stay the same across Flink versions. Choose the artifactId from
the section you are using, then choose the `<version>` from the table.

Make sure the version matches:

- the Cobble version you want to use
- the Flink minor version of your cluster
- the artifact form you choose to use

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
cp cobble-flink-dist-0.2.0-1-flink-1.17.jar "$FLINK_HOME/lib/"
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
    <version>0.2.0-1-flink-1.19</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-source</artifactId>
    <version>0.2.0-1-flink-1.17</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-sink</artifactId>
    <version>0.2.0-1-flink-1.17</version>
  </dependency>
</dependencies>
```

Common choices:

- stateful DataStream job: `cobble-flink-state`
- SQL read job: `cobble-flink-source`
- SQL write job: `cobble-flink-sink`
- mixed usage: add multiple dependencies

### Configure Flink if you use the state backend

If you want to use Cobble as the Flink state backend, configure Flink like this:

```yaml
state.backend.type: io.cobble.flink.state.CobbleStateBackendFactory
state.backend.cobble.localdir: /tmp/flink-cobble/local
state.backend.cobble.memory.managed: true

state.checkpoints.dir: hdfs:///user/you/checkpoints

# Cobble handles the outer HA integration
high-availability.type: io.cobble.flink.state.CobbleHighAvailabilityServicesFactory
# If you already had another HA type before, put it here
cobble.ha.delegate.type: NONE
```

If your Flink daemons run on Java 11 or newer, also add:

```yaml
env.java.opts.all: >-
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
```

If multiple JDKs are installed, pin the runtime explicitly if needed:

```yaml
env.java.home: /path/to/your/jdk
```

If your job only uses source or sink, you do not need this state-backend
configuration.

### Start Flink and submit the job

1. If you chose the runtime-jar setup, put the Cobble jar into `$FLINK_HOME/lib`
2. Update `$FLINK_HOME/conf/flink-conf.yaml` if you use the state backend
3. Start the cluster with `$FLINK_HOME/bin/start-cluster.sh`
4. Submit your job with `$FLINK_HOME/bin/flink run ...`

## Where To Go Next

- [State Backend](../state-backend/) if your job is stateful
- [Source](../source/) if you want to read from Cobble
- [Sink](../sink/) if you want to write into Cobble
