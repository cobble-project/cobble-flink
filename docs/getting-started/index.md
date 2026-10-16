---
title: Getting Started
nav_order: 3
has_children: true
---

# Getting Started

## Setup

Use an installed Flink distribution and its supported Java runtime. Choose the
artifact for your Flink version on the [Versions](versions.html) page.

### Option A: download the runtime jar (recommended)

1. Open the [Maven Central download directory](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-dist/).
2. Choose the version matching your Flink installation, then download
   `cobble-flink-dist-<version>.jar` (not the sources or javadoc jar).
3. Copy it into `$FLINK_HOME/lib/` on each Flink node, then restart Flink.

For example, for Flink 1.17 or 1.18, download
[cobble-flink-dist-0.5.3-1-flink-1.17.jar](https://repo.maven.apache.org/maven2/io/github/cobble-project/cobble-flink-dist/0.5.3-1-flink-1.17/cobble-flink-dist-0.5.3-1-flink-1.17.jar):

```bash
export FLINK_HOME=/path/to/flink
cp ~/Downloads/cobble-flink-dist-0.5.3-1-flink-1.17.jar "$FLINK_HOME/lib/"
```

The bundle includes the state backend, SQL source, and SQL sink. Install only
one matching Cobble dist jar in `lib/`.

### Option B: package it with your job

If you package dependencies with your own Flink job, add the dist bundle to
`pom.xml` instead of installing it in the cluster. This example is for Flink
1.17 or 1.18; see [Versions](versions.html) for other releases or individual
state/source/sink dependencies.

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-dist</artifactId>
  <version>0.5.3-1-flink-1.17</version>
</dependency>
```

## Try It: Write and Read a Table

The following example uses **Option A** and a local, single-machine Flink
cluster. Use a new empty directory for the table. For a distributed cluster,
use shared storage accessible to every TaskManager; see [remote storage](../sink/#remote-storage).

Start Flink if it is not already running, then open the SQL Client:

```bash
"$FLINK_HOME/bin/start-cluster.sh"
"$FLINK_HOME/bin/sql-client.sh"
```

### Write two rows

Run the following in the SQL Client. Batch mode commits the table when the
insert finishes, and synchronous DML waits for that completion.

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

CREATE TABLE demo (
  id BIGINT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'path' = 'file:///tmp/cobble-quickstart',
  'bucket' = '1',
  'sink.parallelism' = '1'
);

INSERT INTO demo VALUES (1, 'Alice'), (2, 'Bob');
```

### Read them back

Read the completed snapshot using the same schema and path:

```sql
CREATE TABLE demo_read (
  id BIGINT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'cobble',
  'source.kind' = 'sink',
  'path' = 'file:///tmp/cobble-quickstart',
  'scan.checkpoint-id' = 'latest',
  'scan.mode' = 'batch'
);

SELECT * FROM demo_read;
```

You should see `1, Alice` and `2, Bob` (row order may vary). No state-backend
configuration is needed for this source/sink example.

## Use Cobble for Streaming State

To enable Cobble for a stateful job, follow the
[state backend setup](../state-backend/#installation-and-setup). That guide
covers checkpoint storage, required JVM settings, and migration from RocksDB.
Restore and rescale use Flink's `CLAIM` restore mode.

## Next Steps

- [Sink](../sink/): persist streaming SQL results; enable checkpointing for streaming writes.
- [Source](../source/): scan snapshots, read state, and perform lookup joins.
- [Web Monitor](../web-monitor/): inspect stored records in your browser.
- [Versions](versions.html): runtime, dependency, and Monitor compatibility.
