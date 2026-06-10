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

Download the released `cobble-flink-dist-<version>.jar` from a Maven
repository and place it in the Flink distribution's `lib/` directory:

```bash
export FLINK_HOME=/path/to/flink-1.17.x
cp cobble-flink-dist-<version>.jar "$FLINK_HOME/lib/"
```

If you are not using a released jar yet and want to build from source, you can
build the distribution jar locally:

```bash
./mvnw --batch-mode --no-transfer-progress -DskipTests package

cp cobble-dist/target/cobble-flink-dist-*.jar "$FLINK_HOME/lib/"
```

For normal users, the important point is simple: the Flink cluster should use
the bundled `dist` jar.

### Option B: use job-side Maven dependencies

For your own job, depend on the modules you actually use instead of the bundled
`dist` jar.

Example `pom.xml` snippet:

```xml
<properties>
  <cobble.flink.version>${cobble-version}-{minor-version}-flink-{flink-minor-version}</cobble.flink.version>
</properties>

<dependencies>
  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-state</artifactId>
    <version>${cobble.flink.version}</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-source</artifactId>
    <version>${cobble.flink.version}</version>
  </dependency>

  <dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble-flink-sink</artifactId>
    <version>${cobble.flink.version}</version>
  </dependency>
</dependencies>
```

Common choices:

- stateful DataStream job: `cobble-flink-state`
- SQL read job: `cobble-flink-source`
- SQL write job: `cobble-flink-sink`
- mixed usage: add multiple modules

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
