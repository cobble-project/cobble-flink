package io.cobble.flink.table;

import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Resolves persisted sink inspect sidecars for Cobble sink source reads. */
final class SinkSourceSchemaResolver {

    private static final String INSPECT_SCHEMA = "inspect-schema";
    private static final String EVENTS = "events";
    private static final String BLOBS = "blobs";

    private SinkSourceSchemaResolver() {}

    static SinkSourceResolvedSchema resolve(
            String pathUri, String scanCheckpointId, ResolvedSchema ddlSchema) {
        Path schemaRoot = new Path(new Path(pathUri), INSPECT_SCHEMA);
        Path eventsDir = new Path(schemaRoot, EVENTS);
        FileSystem fs = fileSystem(eventsDir, pathUri);

        List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents(fs, eventsDir, pathUri);
        if (events.isEmpty()) {
            return SinkSourceResolvedSchema.absent();
        }

        InspectSchemaRegistryLayout.SchemaEvent event =
                selectEvent(events, scanCheckpointId, pathUri);
        SinkInspectSchemaStore store = readStore(fs, new Path(schemaRoot, BLOBS), event);
        if (store.isEmpty()) {
            throw new ValidationException(
                    "Cobble sink inspect schema blob for snapshot "
                            + event.checkpointId()
                            + " parsed as an empty store; refusing to fall back to DDL-derived"
                            + " schema because a sidecar event exists.");
        }

        SinkInspectSchema schema = store.schema();
        validateDdl(schema, ddlSchema);
        return SinkSourceResolvedSchema.present(
                event.checkpointId(), keyFields(schema), valueFields(schema));
    }

    private static InspectSchemaRegistryLayout.SchemaEvent selectEvent(
            List<InspectSchemaRegistryLayout.SchemaEvent> events,
            String scanCheckpointId,
            String pathUri) {
        InspectSchemaRegistryLayout.SchemaEvent best = null;
        if ("latest".equals(scanCheckpointId)) {
            for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
                if (best == null || event.checkpointId() > best.checkpointId()) {
                    best = event;
                }
            }
            return best;
        }

        long selected = Long.parseLong(scanCheckpointId);
        for (InspectSchemaRegistryLayout.SchemaEvent event : events) {
            if (event.checkpointId() <= selected
                    && (best == null || event.checkpointId() > best.checkpointId())) {
                best = event;
            }
        }
        if (best == null) {
            throw new ValidationException(
                    "No Cobble sink inspect schema event with snapshot id <= "
                            + selected
                            + " was found under "
                            + pathUri
                            + "/"
                            + INSPECT_SCHEMA
                            + ". Adjust scan.checkpoint-id or use 'latest'.");
        }
        return best;
    }

    private static SinkInspectSchemaStore readStore(
            FileSystem fs, Path blobsDir, InspectSchemaRegistryLayout.SchemaEvent event) {
        Path blobPath = new Path(blobsDir, InspectSchemaRegistryLayout.blobFileName(event.hash()));
        try {
            if (!fs.exists(blobPath)) {
                throw new ValidationException(
                        "Cobble sink inspect schema blob is missing for hash "
                                + event.hash()
                                + " (expected at "
                                + blobPath
                                + ").");
            }
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (FSDataInputStream input = fs.open(blobPath)) {
                byte[] chunk = new byte[8 * 1024];
                int read;
                while ((read = input.read(chunk)) >= 0) {
                    buffer.write(chunk, 0, read);
                }
            }
            return SinkInspectSchemaStore.fromBytes(buffer.toByteArray());
        } catch (ValidationException e) {
            throw e;
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to read Cobble sink inspect schema blob "
                            + blobPath
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static void validateDdl(SinkInspectSchema schema, ResolvedSchema ddlSchema) {
        UniqueConstraint primaryKey =
                ddlSchema
                        .getPrimaryKey()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Cobble sink source with an inspect schema sidecar"
                                                        + " requires a PRIMARY KEY matching the"
                                                        + " persisted key fields."));

        List<SinkInspectField> keyFields = schema.keyFields();
        List<String> pkColumns = primaryKey.getColumns();
        if (pkColumns.size() != keyFields.size()) {
            throw new ValidationException(
                    "Cobble sink source PRIMARY KEY has "
                            + pkColumns.size()
                            + " column(s), but the inspect schema expects "
                            + keyFields.size()
                            + ": "
                            + describeNames(keyFields)
                            + ".");
        }
        for (int i = 0; i < keyFields.size(); i++) {
            String actual = pkColumns.get(i);
            String expected = keyFields.get(i).name();
            if (!actual.equals(expected)) {
                throw new ValidationException(
                        "Cobble sink source PRIMARY KEY column at position "
                                + i
                                + " is '"
                                + actual
                                + "' but the inspect schema expects '"
                                + expected
                                + "'.");
            }
        }

        RowType rowType = (RowType) ddlSchema.toPhysicalRowDataType().getLogicalType();
        List<RowType.RowField> actualFields = rowType.getFields();
        List<SinkInspectField> expectedFields =
                expectedFieldsByRowIndex(schema, actualFields.size());
        validateStructuredColumnIndexes(schema.valueFields());

        for (int i = 0; i < actualFields.size(); i++) {
            RowType.RowField actual = actualFields.get(i);
            SinkInspectField expected = expectedFields.get(i);
            if (!actual.getName().equals(expected.name())) {
                throw new ValidationException(
                        "Cobble sink source column at position "
                                + i
                                + " is '"
                                + actual.getName()
                                + "' but the inspect schema expects '"
                                + expected.name()
                                + "'. The DDL physical column order must match the persisted sink"
                                + " schema rowIndex layout. Expected columns: "
                                + describeFields(expectedFields)
                                + ".");
            }
            if (!typesMatch(expected.logicalType(), actual.getType())) {
                throw new ValidationException(
                        "Cobble sink source column '"
                                + actual.getName()
                                + "' has type "
                                + actual.getType().asSerializableString()
                                + " but the inspect schema expects "
                                + expected.logicalType()
                                + ".");
            }
        }
    }

    private static List<SinkInspectField> expectedFieldsByRowIndex(
            SinkInspectSchema schema, int physicalFieldCount) {
        List<SinkInspectField> allFields = new ArrayList<>();
        allFields.addAll(schema.keyFields());
        allFields.addAll(schema.valueFields());
        SinkInspectField[] byRowIndex = new SinkInspectField[physicalFieldCount];
        for (SinkInspectField field : allFields) {
            int rowIndex = field.rowIndex();
            if (rowIndex < 0 || rowIndex >= physicalFieldCount) {
                throw new ValidationException(
                        "Cobble sink inspect schema field '"
                                + field.name()
                                + "' has rowIndex "
                                + rowIndex
                                + ", but the DDL has "
                                + physicalFieldCount
                                + " physical column(s).");
            }
            SinkInspectField previous = byRowIndex[rowIndex];
            if (previous != null) {
                throw new ValidationException(
                        "Cobble sink inspect schema has duplicate rowIndex "
                                + rowIndex
                                + " for fields '"
                                + previous.name()
                                + "' and '"
                                + field.name()
                                + "'.");
            }
            byRowIndex[rowIndex] = field;
        }
        List<SinkInspectField> expectedFields = new ArrayList<>(physicalFieldCount);
        for (int rowIndex = 0; rowIndex < byRowIndex.length; rowIndex++) {
            SinkInspectField field = byRowIndex[rowIndex];
            if (field == null) {
                throw new ValidationException(
                        "Cobble sink inspect schema has no field for physical column position "
                                + rowIndex
                                + ". Every DDL physical column must map to exactly one sidecar"
                                + " field.");
            }
            expectedFields.add(field);
        }
        return expectedFields;
    }

    private static void validateStructuredColumnIndexes(List<SinkInspectField> valueFields) {
        boolean[] seen = new boolean[valueFields.size()];
        for (SinkInspectField field : valueFields) {
            int structuredColumnIndex = field.structuredColumnIndex();
            if (structuredColumnIndex < 0 || structuredColumnIndex >= valueFields.size()) {
                throw new ValidationException(
                        "Cobble sink inspect schema value field '"
                                + field.name()
                                + "' has structuredColumnIndex "
                                + structuredColumnIndex
                                + ", but value fields require indexes in [0, "
                                + (valueFields.size() - 1)
                                + "].");
            }
            if (seen[structuredColumnIndex]) {
                throw new ValidationException(
                        "Cobble sink inspect schema has duplicate value structuredColumnIndex "
                                + structuredColumnIndex
                                + ".");
            }
            seen[structuredColumnIndex] = true;
        }
    }

    private static List<CobbleDynamicTableSource.SerializableField> keyFields(
            SinkInspectSchema schema) {
        List<CobbleDynamicTableSource.SerializableField> fields = new ArrayList<>();
        for (SinkInspectField field : schema.keyFields()) {
            fields.add(
                    new CobbleDynamicTableSource.SerializableField(
                            field.name(), field.logicalType(), field.rowIndex(), -1));
        }
        return fields;
    }

    private static List<CobbleDynamicTableSource.SerializableField> valueFields(
            SinkInspectSchema schema) {
        List<CobbleDynamicTableSource.SerializableField> fields = new ArrayList<>();
        for (SinkInspectField field : schema.valueFields()) {
            fields.add(
                    new CobbleDynamicTableSource.SerializableField(
                            field.name(),
                            field.logicalType(),
                            field.rowIndex(),
                            field.structuredColumnIndex()));
        }
        return fields;
    }

    private static boolean typesMatch(String expectedLogicalType, LogicalType actualType) {
        try {
            LogicalType expected =
                    LogicalTypeParser.parse(
                            expectedLogicalType, SinkSourceSchemaResolver.class.getClassLoader());
            LogicalType nullableExpected = expected.copy(true);
            LogicalType nullableActual = actualType.copy(true);
            if (nullableExpected.getTypeRoot() == LogicalTypeRoot.VARCHAR
                    && nullableActual.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                return true;
            }
            return nullableExpected
                    .asSerializableString()
                    .equals(nullableActual.asSerializableString());
        } catch (RuntimeException e) {
            return expectedLogicalType.equals(actualType.asSerializableString());
        }
    }

    private static String describeFields(List<SinkInspectField> fields) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            SinkInspectField field = fields.get(i);
            builder.append(field.name()).append(' ').append(field.logicalType());
        }
        return builder.append(']').toString();
    }

    private static String describeNames(List<SinkInspectField> fields) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(fields.get(i).name());
        }
        return builder.append(']').toString();
    }

    private static List<InspectSchemaRegistryLayout.SchemaEvent> listEvents(
            FileSystem fs, Path eventsDir, String pathUri) {
        FileStatus[] statuses = listStatus(fs, eventsDir, pathUri);
        List<InspectSchemaRegistryLayout.SchemaEvent> events = new ArrayList<>();
        if (statuses == null) {
            return events;
        }
        for (FileStatus status : statuses) {
            if (status.isDir()) {
                continue;
            }
            InspectSchemaRegistryLayout.SchemaEvent event =
                    InspectSchemaRegistryLayout.parseEventFileName(status.getPath().getName());
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    private static FileSystem fileSystem(Path path, String pathUri) {
        try {
            return path.getFileSystem();
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to open filesystem for Cobble sink source path "
                            + pathUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }

    private static FileStatus[] listStatus(FileSystem fs, Path dir, String pathUri) {
        try {
            if (!fs.exists(dir)) {
                return null;
            }
            return fs.listStatus(dir);
        } catch (IOException e) {
            throw new ValidationException(
                    "Failed to list "
                            + dir
                            + " under Cobble sink source path "
                            + pathUri
                            + ": "
                            + e.getMessage(),
                    e);
        }
    }
}

final class SinkSourceResolvedSchema {

    private final boolean present;
    private final long schemaSnapshotId;
    private final List<CobbleDynamicTableSource.SerializableField> keyFields;
    private final List<CobbleDynamicTableSource.SerializableField> valueFields;

    private SinkSourceResolvedSchema(
            boolean present,
            long schemaSnapshotId,
            List<CobbleDynamicTableSource.SerializableField> keyFields,
            List<CobbleDynamicTableSource.SerializableField> valueFields) {
        this.present = present;
        this.schemaSnapshotId = schemaSnapshotId;
        this.keyFields = keyFields;
        this.valueFields = valueFields;
    }

    static SinkSourceResolvedSchema absent() {
        return new SinkSourceResolvedSchema(
                false, -1L, java.util.Collections.emptyList(), java.util.Collections.emptyList());
    }

    static SinkSourceResolvedSchema present(
            long schemaSnapshotId,
            List<CobbleDynamicTableSource.SerializableField> keyFields,
            List<CobbleDynamicTableSource.SerializableField> valueFields) {
        return new SinkSourceResolvedSchema(true, schemaSnapshotId, keyFields, valueFields);
    }

    boolean present() {
        return present;
    }

    long schemaSnapshotId() {
        return schemaSnapshotId;
    }

    List<CobbleDynamicTableSource.SerializableField> keyFields() {
        return keyFields;
    }

    List<CobbleDynamicTableSource.SerializableField> valueFields() {
        return valueFields;
    }
}
