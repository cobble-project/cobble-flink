package io.cobble.flink.inspect.internal;

import io.cobble.flink.common.inspect.SinkInspectField;
import io.cobble.flink.common.inspect.StateInspectField;
import io.cobble.flink.common.inspect.StateInspectType;
import io.cobble.flink.common.inspect.StateInspectTypeKind;
import io.cobble.flink.inspect.FieldValue;
import io.cobble.flink.inspect.TypedValue;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Strict validation and stable identity for public typed field inputs. */
final class TypedInputs {
    private TypedInputs() {}

    static List<String> sink(
            List<FieldValue> actual, List<SinkInspectField> expected, boolean complete) {
        List<FieldSpec> fields = new ArrayList<>();
        for (SinkInspectField field : expected) {
            fields.add(new FieldSpec(field.name(), field.logicalType()));
        }
        return validate(actual, fields, complete, "sink primary key");
    }

    static List<String> semantic(
            List<FieldValue> actual,
            StateInspectType type,
            String scalarName,
            boolean complete,
            String label) {
        return validate(actual, semanticFields(type, scalarName, label), complete, label);
    }

    static String identity(List<FieldValue> fields) {
        if (fields == null || fields.isEmpty()) {
            return "-";
        }
        StringBuilder value = new StringBuilder();
        for (FieldValue field : fields) {
            requireField(field, "typed filter");
            value.append(field.name().length())
                    .append(':')
                    .append(field.name())
                    .append(':')
                    .append(field.value().kind())
                    .append(':')
                    .append(field.value().text().length())
                    .append(':')
                    .append(field.value().text())
                    .append(';');
        }
        return value.toString();
    }

    private static List<String> validate(
            List<FieldValue> actual, List<FieldSpec> expected, boolean complete, String label) {
        List<FieldValue> values = actual == null ? Collections.<FieldValue>emptyList() : actual;
        if (complete && values.size() != expected.size()) {
            throw InspectSessionImpl.invalid(
                    "Typed " + label + " requires all fields in order: " + names(expected));
        }
        if (values.size() > expected.size()) {
            throw InspectSessionImpl.invalid(
                    "Typed " + label + " has too many fields; expected " + names(expected));
        }
        List<String> texts = new ArrayList<>();
        for (int index = 0; index < values.size(); index++) {
            FieldValue actualField = values.get(index);
            requireField(actualField, label);
            FieldSpec expectedField = expected.get(index);
            if (!expectedField.name.equals(actualField.name())) {
                throw InspectSessionImpl.invalid(
                        "Typed "
                                + label
                                + " field at position "
                                + index
                                + " must be '"
                                + expectedField.name
                                + "' but was '"
                                + actualField.name()
                                + "'");
            }
            validateType(actualField.value(), expectedField.logicalType, actualField.name());
            texts.add(actualField.value().text());
        }
        return texts;
    }

    private static void requireField(FieldValue field, String label) {
        if (field == null || field.name() == null || field.value() == null) {
            throw InspectSessionImpl.invalid(label + " fields and values must not be null");
        }
    }

    private static List<FieldSpec> semanticFields(
            StateInspectType type, String scalarName, String label) {
        if (type == null || type.kind() == StateInspectTypeKind.UNKNOWN) {
            throw InspectSessionImpl.invalid("Semantic metadata is unavailable for " + label);
        }
        List<FieldSpec> fields = new ArrayList<>();
        if (type.kind() == StateInspectTypeKind.SCALAR) {
            fields.add(new FieldSpec(scalarName, type.logicalType()));
            return fields;
        }
        if (type.kind() != StateInspectTypeKind.ROW && type.kind() != StateInspectTypeKind.TUPLE) {
            throw InspectSessionImpl.invalid(
                    "Typed " + label + " does not support " + type.kind() + " values");
        }
        for (StateInspectField field : type.fields()) {
            if (field.type().kind() != StateInspectTypeKind.SCALAR) {
                throw InspectSessionImpl.invalid(
                        "Typed " + label + " does not support nested field " + field.name());
            }
            fields.add(new FieldSpec(field.name(), field.type().logicalType()));
        }
        return fields;
    }

    private static void validateType(TypedValue value, String logicalType, String fieldName) {
        final LogicalTypeRoot root;
        try {
            LogicalType parsed =
                    LogicalTypeParser.parse(logicalType, TypedInputs.class.getClassLoader());
            root = parsed.getTypeRoot();
            SinkInspectDecoder.parseFieldInput(parsed, value.text());
        } catch (Exception error) {
            throw InspectSessionImpl.invalid(
                    "Invalid value for typed field '"
                            + fieldName
                            + "' ("
                            + logicalType
                            + "): "
                            + InspectSessionImpl.message(error));
        }
        boolean accepted;
        switch (root) {
            case CHAR:
            case VARCHAR:
                accepted = value.kind() == TypedValue.Kind.STRING;
                break;
            case BOOLEAN:
                accepted = value.kind() == TypedValue.Kind.BOOLEAN;
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                accepted = value.kind() == TypedValue.Kind.INTEGER;
                break;
            case FLOAT:
            case DOUBLE:
                accepted = value.kind() == TypedValue.Kind.FLOAT;
                break;
            case DECIMAL:
                accepted = value.kind() == TypedValue.Kind.DECIMAL;
                break;
            case BINARY:
            case VARBINARY:
                accepted = value.kind() == TypedValue.Kind.BYTES;
                break;
            case DATE:
                accepted = value.kind() == TypedValue.Kind.DATE;
                break;
            case TIME_WITHOUT_TIME_ZONE:
                accepted = value.kind() == TypedValue.Kind.TIME;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                accepted = value.kind() == TypedValue.Kind.TIMESTAMP;
                break;
            default:
                accepted = false;
        }
        if (!accepted) {
            throw InspectSessionImpl.invalid(
                    "Typed field '"
                            + fieldName
                            + "' expects "
                            + logicalType
                            + " but received "
                            + value.kind());
        }
    }

    private static String names(List<FieldSpec> fields) {
        StringBuilder names = new StringBuilder();
        for (int index = 0; index < fields.size(); index++) {
            if (index > 0) {
                names.append(", ");
            }
            names.append(fields.get(index).name);
        }
        return names.toString();
    }

    private static final class FieldSpec {
        private final String name;
        private final String logicalType;

        private FieldSpec(String name, String logicalType) {
            this.name = name;
            this.logicalType = logicalType;
        }
    }
}
