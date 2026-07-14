package io.cobble.flink.table;

import io.cobble.flink.common.CobbleConnectorStorageOptions;
import io.cobble.flink.common.CobbleMetadataFileIO;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.SinkInspectSchema;
import io.cobble.flink.common.inspect.SinkInspectSchemaStore;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

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
        return resolve(pathUri, scanCheckpointId, ddlSchema, CobbleConnectorStorageOptions.empty());
    }

    static SinkSourceResolvedSchema resolve(
            String pathUri,
            String scanCheckpointId,
            ResolvedSchema ddlSchema,
            CobbleConnectorStorageOptions storageOptions) {
        String eventsDir = INSPECT_SCHEMA + "/" + EVENTS;
        String blobsDir = INSPECT_SCHEMA + "/" + BLOBS;
        try {
            CobbleMetadataFileIO fileIO = CobbleMetadataFileIO.open(pathUri, storageOptions);
            List<InspectSchemaRegistryLayout.SchemaEvent> events = listEvents(fileIO, eventsDir);
            if (events.isEmpty()) {
                return SinkSourceResolvedSchema.absent();
            }

            InspectSchemaRegistryLayout.SchemaEvent event =
                    selectEvent(events, scanCheckpointId, pathUri);
            SinkInspectSchemaStore store = readStore(fileIO, blobsDir, event);
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
        } catch (ValidationException e) {
            throw e;
        } catch (IOException e) {
            throw new ValidationException("Failed to access Cobble sink inspect schema.", e);
        }
    }

    private static SinkInspectSchemaStore readStore(
            CobbleMetadataFileIO fileIO,
            String blobsDir,
            InspectSchemaRegistryLayout.SchemaEvent event)
            throws IOException {
        String blobPath = blobsDir + "/" + InspectSchemaRegistryLayout.blobFileName(event.hash());
        if (!fileIO.exists(blobPath)) {
            throw new ValidationException(
                    "Cobble sink inspect schema blob is missing for hash " + event.hash() + '.');
        }
        return SinkInspectSchemaStore.fromBytes(fileIO.read(blobPath));
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
            CobbleMetadataFileIO fileIO, String eventsDir) throws IOException {
        List<InspectSchemaRegistryLayout.SchemaEvent> events = new ArrayList<>();
        for (String name : fileIO.list(eventsDir)) {
            InspectSchemaRegistryLayout.SchemaEvent event =
                    InspectSchemaRegistryLayout.parseEventFileName(name);
            if (event != null) {
                events.add(event);
            }
        }
        return events;
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
