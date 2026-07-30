---
title: Java Inspect SDK
parent: Web Monitor
nav_order: 4
---

# Java Inspect SDK

Use `cobble-flink-inspect` to embed the same read-only discovery, Overview,
scan, lookup, schema decoding, and SQL generation used by the web monitor in a
Java application. No HTTP server is required.

## Dependency

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-inspect</artifactId>
  <version>0.3.0-1-flink-1.17</version>
</dependency>
```

The inspect artifact is shared across supported Flink versions. If you run it
inside an existing Flink process, use that process's filesystem setup. For a
standalone application that needs Flink filesystem plugins, provide its Flink
configuration directory with `flinkConfigPath`.

## Discover And Open

The client and every open session own native resources. Always close them with
`try-with-resources`:

```java
import io.cobble.flink.inspect.CheckpointSelection;
import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;

try (CobbleInspectClient client =
        CobbleInspectClient.builder()
                .flinkConfigPath("/path/to/flink/conf")
                .build()) {
    InspectCatalog catalog = client.discover("file:///path/to/checkpoints");

    InspectSelection selection =
            InspectSelection.builder("file:///path/to/checkpoints")
                    .checkpoint(CheckpointSelection.latest())
                    .operatorId("<operator-id>")
                    .build();

    try (InspectSession session = client.open(selection)) {
        System.out.println(session.info().selection().checkpointId());
        session.overview().items().forEach(
                item -> System.out.println(item.sourceSql().ddl()));
    }
}
```

`latest` is resolved once when the session opens. The numeric checkpoint or
snapshot id in `session.info()` and generated SQL stays fixed until you open a
new session.

## Scan And Pagination

```java
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.ScanRequest;

InspectPage page = session.scan(new ScanRequest("orders", 100, null));

while (page.nextPageToken() != null) {
    page = session.scan(
            new ScanRequest("orders", 100, page.nextPageToken()));
}
```

Rows preserve raw bytes, decoded fields, and row-level decode issues. A value
that cannot be decoded does not stop the remaining rows from being returned.

For a raw prefix scan, include a `RawBytes` prefix and optional bucket or column
projection in the full `ScanRequest` constructor. Use `ScanFilter.sink(...)`
for ordered sink key fields, or `ScanFilter.state(...)` for a state key,
namespace, and optional MapState key.

## Exact Lookup

Raw lookup uses the key group or bucket plus the exact key bytes:

```java
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.RawBytes;

LookupRequest request = new LookupRequest(
        "raw",
        java.util.Collections.singletonList(
                new LookupKey(0, new RawBytes("alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8)))));
LookupResult result = session.lookup(request);
```

Schema-aware lookup uses `LookupKey.typed(...)`. Provide the complete sink
primary key, or the complete state key plus namespace and MapState key when
required. `session.overview()` reports whether exact lookup is supported and
lists the required fields.

## Storage Options And User Jars

For remote storage, pass the same provider options used by the connector:

```java
java.util.Map<String, String> options = new java.util.HashMap<>();
options.put("storage.option.endpoint", "https://s3.example.com");
options.put("storage.option.region", "us-east-1");

CobbleInspectClient client = CobbleInspectClient.builder()
        .storageOptions(
                io.cobble.flink.common.CobbleConnectorStorageOptions
                        .fromStorageOptions(options))
        .build();
```

Keep credentials in application configuration or secret management rather
than embedding them in the source URI.

Standard semantic types are decoded without job jars when their serializer
snapshots contain enough information. Add trusted job or dependency jars only
when a custom serializer needs them:

```java
CobbleInspectClient client = CobbleInspectClient.builder()
        .userJars(java.util.Arrays.asList(
                "/path/to/job.jar",
                "/path/to/dependency.jar"))
        .build();
```
