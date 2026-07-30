# Inspect SDK Probe Reference

## Dependency

Use the current repository version when working in this checkout. For released consumers:

```xml
<dependency>
  <groupId>io.github.cobble-project</groupId>
  <artifactId>cobble-flink-inspect</artifactId>
  <version>0.3.0-1-flink-1.17</version>
</dependency>
```

The Inspect SDK artifact is shared across supported Flink versions.

## Minimal Probe

```java
import io.cobble.flink.inspect.CheckpointInfo;
import io.cobble.flink.inspect.CobbleInspectClient;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectOverviewItem;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.InspectTarget;
import io.cobble.flink.inspect.OperatorInfo;
import io.cobble.flink.inspect.ScanRequest;

String sourcePath = args[0];

try (CobbleInspectClient client = CobbleInspectClient.builder().build()) {
    InspectCatalog catalog = client.discover(sourcePath);
    System.out.println("sourceKind=" + catalog.sourceKind());
    System.out.println("root=" + catalog.rootDirectory());
    for (CheckpointInfo checkpoint : catalog.checkpoints()) {
        System.out.println("checkpoint=" + checkpoint.checkpointId());
        for (OperatorInfo operator : checkpoint.operators()) {
            System.out.println("  operator=" + operator.operatorId());
        }
    }

    InspectSelection selection = InspectSelection.latest(sourcePath, null);
    try (InspectSession session = client.open(selection)) {
        System.out.println(
                "pinnedCheckpoint=" + session.info().selection().checkpointId());
        System.out.println("operator=" + session.info().selection().operatorId());

        for (InspectOverviewItem item : session.overview().items()) {
            System.out.println("overview=" + item.id() + " kind=" + item.kind());
            if (item.sourceSql().available()) {
                System.out.println(item.sourceSql().ddl());
            } else {
                System.out.println("sqlUnavailable=" + item.sourceSql().unavailableReason());
            }
        }

        for (InspectTarget target : session.targets()) {
            System.out.println(
                    "target=" + target.id()
                            + " kind=" + target.kind()
                            + " stateKind=" + target.stateKind());
            InspectPage page = session.scan(new ScanRequest(target.id(), 20, null));
            System.out.println("rows=" + page.rows().size());
            page.rows().forEach(
                    row -> System.out.println(
                            "  bucket=" + row.bucket()
                                    + " decodedParts=" + row.decodedParts()
                                    + " decodeIssues=" + row.decodeIssues()));
        }
    }
}
```

When discovery reports multiple operators, replace `null` with the chosen operator ID rather than
relying on the default operator.

## Exact Checkpoint

```java
InspectSelection selection = InspectSelection.checkpoint(sourcePath, checkpointId, operatorId);
```

An exact selection is immutable and reproducible. If it has expired or become incomplete, report
the failure; do not switch to another checkpoint.

## Remote Storage And Flink Filesystems

```java
import io.cobble.flink.common.CobbleConnectorStorageOptions;

java.util.Map<String, String> options = new java.util.HashMap<>();
options.put("storage.option.endpoint", "https://s3.example.com");
options.put("storage.option.region", "us-east-1");

CobbleInspectClient client = CobbleInspectClient.builder()
        .flinkConfigPath("/path/to/flink/conf")
        .storageOptions(CobbleConnectorStorageOptions.fromStorageOptions(options))
        .build();
```

Pass credentials through environment-backed application configuration or secret management. Do
not print them in probe output.

## User Jars

```java
CobbleInspectClient client = CobbleInspectClient.builder()
        .userJars(java.util.Arrays.asList("/path/job.jar", "/path/dependency.jar"))
        .build();
```

Run once without user jars before adding them. This shows whether serializer snapshots and
classless decoders are sufficient by themselves.

## Pagination

```java
InspectPage page = session.scan(new ScanRequest(targetId, 100, null));
while (page.nextPageToken() != null) {
    page = session.scan(new ScanRequest(targetId, 100, page.nextPageToken()));
}
```

Do not use this loop for the default health probe. It may scan an entire state or sink table.

## Failure Categories

Catch `InspectException` and report `errorCode()` plus the message:

- `INVALID_INPUT`: malformed selection/filter.
- `NOT_FOUND`: no matching checkpoint, operator, or target.
- `CHECKPOINT_UNAVAILABLE`: selected files were removed or became incomplete.
- `UNREADABLE`: filesystem, metadata, or native-reader failure.
- `UNSUPPORTED`: operation is not available for the selected target.
- `CLOSED`: client/session lifecycle error.
- `INTERNAL`: unexpected SDK failure.

Keep row-level `DecodeIssue` values separate from request-level exceptions.
