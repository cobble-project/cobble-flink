package io.cobble.flink.monitor;

import io.cobble.flink.inspect.CheckpointInfo;
import io.cobble.flink.inspect.CheckpointSelection;
import io.cobble.flink.inspect.DecodeIssue;
import io.cobble.flink.inspect.DecodedValue;
import io.cobble.flink.inspect.FieldValue;
import io.cobble.flink.inspect.InspectCatalog;
import io.cobble.flink.inspect.InspectOverview;
import io.cobble.flink.inspect.InspectOverviewItem;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectRow;
import io.cobble.flink.inspect.InspectSelection;
import io.cobble.flink.inspect.InspectSession;
import io.cobble.flink.inspect.InspectTarget;
import io.cobble.flink.inspect.LookupKey;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.LookupResult;
import io.cobble.flink.inspect.OperatorInfo;
import io.cobble.flink.inspect.PageToken;
import io.cobble.flink.inspect.RawBytes;
import io.cobble.flink.inspect.ScanFilter;
import io.cobble.flink.inspect.ScanRequest;
import io.cobble.flink.inspect.SemanticField;
import io.cobble.flink.inspect.SemanticType;
import io.cobble.flink.inspect.SourceSqlExample;
import io.cobble.flink.inspect.StateKey;
import io.cobble.flink.inspect.TypedLookupKey;
import io.cobble.flink.inspect.TypedValue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Explicit transport adapters. SDK domain objects are never reflectively serialized. */
final class InspectHttpJson {
    private InspectHttpJson() {}

    static InspectSelection selection(JsonObject body) {
        String source = source(body);
        String operator = optionalString(body, "operator_id");
        JsonElement checkpoint = body.get("checkpoint");
        if (checkpoint == null
                || checkpoint.isJsonNull()
                || (checkpoint.isJsonPrimitive()
                        && checkpoint.getAsJsonPrimitive().isString()
                        && "latest".equalsIgnoreCase(checkpoint.getAsString()))) {
            return InspectSelection.builder(source)
                    .checkpoint(CheckpointSelection.latest())
                    .operatorId(operator)
                    .build();
        }
        long id = requiredNonNegativeLong(checkpoint, "checkpoint");
        return InspectSelection.builder(source)
                .checkpoint(CheckpointSelection.checkpoint(id))
                .operatorId(operator)
                .build();
    }

    static String source(JsonObject body) {
        String source = requiredString(body, "source");
        try {
            URI uri = URI.create(source);
            if (uri.getScheme() != null
                    && (uri.getRawUserInfo() != null || uri.getRawQuery() != null)) {
                throw invalid(
                        "source URI credentials and query parameters must be supplied as server storage options");
            }
        } catch (IllegalArgumentException error) {
            throw invalid("source must be a valid path or URI");
        }
        return source;
    }

    static ScanRequest scanRequest(
            JsonObject body, String sessionId, int defaultLimit, int maxLimit) {
        String target = requiredString(body, "target_id");
        int limit = optionalInt(body, "limit", defaultLimit);
        if (limit <= 0 || limit > maxLimit) {
            throw invalid("limit must be between 1 and " + maxLimit);
        }
        Integer bucket = optionalInteger(body, "bucket");
        String wireToken = optionalString(body, "page_token");
        PageToken token =
                wireToken == null ? null : new PageToken(unwrapToken(sessionId, wireToken));
        RawBytes prefix =
                body.has("prefix_b64")
                        ? new RawBytes(
                                decodeBase64(requiredString(body, "prefix_b64"), "prefix_b64"))
                        : null;
        int[] columns = optionalIntArray(body, "columns");
        ScanFilter filter = body.has("filter") ? scanFilter(object(body, "filter")) : null;
        return new ScanRequest(target, limit, token, bucket, prefix, columns, filter);
    }

    static LookupRequest lookupRequest(JsonObject body, int maxKeys) {
        String target = requiredString(body, "target_id");
        JsonArray input = array(body, "keys");
        if (input.size() == 0 || input.size() > maxKeys) {
            throw invalid("keys must contain between 1 and " + maxKeys + " entries");
        }
        List<LookupKey> keys = new ArrayList<>();
        for (JsonElement element : input) {
            JsonObject key = requireObject(element, "keys entry");
            String kind = requiredString(key, "kind").toLowerCase(java.util.Locale.ROOT);
            if ("raw".equals(kind)) {
                keys.add(
                        new LookupKey(
                                requiredInt(key, "bucket"),
                                new RawBytes(
                                        decodeBase64(requiredString(key, "key_b64"), "key_b64"))));
            } else if ("sink".equals(kind)) {
                keys.add(LookupKey.typed(TypedLookupKey.sink(fields(array(key, "fields")))));
            } else if ("state".equals(kind)) {
                keys.add(LookupKey.typed(TypedLookupKey.state(stateKey(object(key, "state_key")))));
            } else {
                throw invalid("lookup key kind must be raw, sink, or state");
            }
        }
        return new LookupRequest(target, keys, optionalIntArray(body, "columns"));
    }

    static JsonObject discovery(InspectCatalog catalog) {
        JsonObject output = new JsonObject();
        output.addProperty("source_kind", catalog.sourceKind());
        output.addProperty("root_directory", catalog.rootDirectory());
        JsonArray checkpoints = new JsonArray();
        for (CheckpointInfo checkpoint : catalog.checkpoints()) {
            JsonObject item = new JsonObject();
            item.addProperty("checkpoint_id", checkpoint.checkpointId());
            item.addProperty("directory", checkpoint.directory());
            JsonArray operators = new JsonArray();
            for (OperatorInfo operator : checkpoint.operators()) {
                JsonObject operatorJson = new JsonObject();
                operatorJson.addProperty("operator_id", operator.operatorId());
                operatorJson.addProperty(
                        "global_snapshot_available", operator.globalSnapshotLayout());
                operatorJson.addProperty(
                        "embedded_metadata_available", operator.embeddedCheckpoint());
                operators.add(operatorJson);
            }
            item.add("operators", operators);
            checkpoints.add(item);
        }
        output.add("checkpoints", checkpoints);
        output.addProperty("available", !catalog.checkpoints().isEmpty());
        return output;
    }

    static JsonObject session(String sessionId, InspectSession session) {
        JsonObject output = new JsonObject();
        output.addProperty("session_id", sessionId);
        output.addProperty("source_kind", session.catalog().sourceKind());
        output.addProperty("root_directory", session.catalog().rootDirectory());
        output.addProperty("source", session.info().selection().sourcePath());
        output.addProperty("checkpoint_id", session.info().selection().checkpointId());
        addNullable(output, "operator_id", session.info().selection().operatorId());
        output.addProperty("pinned", !session.info().selection().latest());
        JsonArray targets = new JsonArray();
        for (InspectTarget target : session.targets()) {
            targets.add(target(target));
        }
        output.add("targets", targets);
        return output;
    }

    static JsonObject overview(InspectOverview overview) {
        JsonObject output = new JsonObject();
        output.addProperty("checkpoint_id", overview.checkpointId());
        addNullable(output, "operator_id", overview.operatorId());
        JsonArray items = new JsonArray();
        for (InspectOverviewItem item : overview.items()) {
            JsonObject value = new JsonObject();
            value.addProperty("id", item.id());
            value.addProperty("title", item.title());
            value.addProperty("kind", item.kind());
            addNullable(value, "detail", item.detail());
            JsonArray fields = new JsonArray();
            for (InspectOverviewItem.Field field : item.fields()) {
                JsonObject fieldJson = new JsonObject();
                fieldJson.addProperty("role", field.role());
                fieldJson.addProperty("name", field.name());
                fieldJson.addProperty("logical_type", field.logicalType());
                fields.add(fieldJson);
            }
            value.add("fields", fields);
            value.add("source_sql", sourceSql(item.sourceSql()));
            items.add(value);
        }
        output.add("items", items);
        return output;
    }

    static JsonObject page(String sessionId, InspectPage page) {
        JsonObject output = new JsonObject();
        output.add("rows", rows(page.rows()));
        if (page.nextPageToken() == null) {
            output.add("next_page_token", JsonNull.INSTANCE);
        } else {
            String wrapped = wrapToken(sessionId, page.nextPageToken().value());
            output.addProperty("next_page_token", wrapped);
        }
        return output;
    }

    static JsonObject lookup(LookupResult result) {
        JsonObject output = new JsonObject();
        output.add("rows", rows(result.rows()));
        return output;
    }

    static String unwrapToken(String sessionId, String token) {
        if (token == null) {
            return null;
        }
        String prefix = "v1." + sessionId + ".";
        if (!token.startsWith(prefix)) {
            throw invalid("page_token belongs to a different session");
        }
        try {
            return new String(
                    Base64.getUrlDecoder().decode(token.substring(prefix.length())),
                    StandardCharsets.UTF_8);
        } catch (IllegalArgumentException error) {
            throw invalid("page_token is malformed");
        }
    }

    private static String wrapToken(String sessionId, String token) {
        return "v1."
                + sessionId
                + "."
                + Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    private static JsonObject target(InspectTarget target) {
        JsonObject output = new JsonObject();
        output.addProperty("id", target.id());
        output.addProperty("name", target.name());
        output.addProperty("kind", target.kind().name().toLowerCase(java.util.Locale.ROOT));
        addNullable(output, "column_family", target.columnFamily());
        addNullable(output, "state_kind", target.stateKind());
        output.addProperty("allows_columns", target.allowsColumns());
        output.addProperty("exact_lookup_supported", target.exactLookupSupported());
        addNullable(output, "exact_lookup_diagnostic", target.exactLookupDiagnostic());
        JsonObject semantic = new JsonObject();
        for (Map.Entry<String, SemanticType> entry : target.semanticSchema().entrySet()) {
            semantic.add(entry.getKey(), semantic(entry.getValue()));
        }
        output.add("semantic_parts", semantic);
        output.add("key_fields", semanticFields(target.sinkKeyFields()));
        output.add("value_fields", semanticFields(target.sinkValueFields()));
        return output;
    }

    private static JsonArray semanticFields(List<SemanticField> fields) {
        JsonArray output = new JsonArray();
        for (SemanticField field : fields) {
            JsonObject item = new JsonObject();
            item.addProperty("name", field.name());
            item.add("type", semantic(field.type()));
            item.addProperty("logical_type", field.type().logicalType());
            output.add(item);
        }
        return output;
    }

    private static JsonObject semantic(SemanticType type) {
        JsonObject output = new JsonObject();
        output.addProperty("kind", type.kind().name());
        addNullable(output, "logical_type", type.logicalType());
        output.add("fields", semanticFields(type.fields()));
        if (type.elementType() != null) output.add("element_type", semantic(type.elementType()));
        if (type.keyType() != null) output.add("key_type", semantic(type.keyType()));
        if (type.valueType() != null) output.add("value_type", semantic(type.valueType()));
        return output;
    }

    private static JsonObject sourceSql(SourceSqlExample sql) {
        JsonObject output = new JsonObject();
        addNullable(output, "ddl", sql.ddl());
        output.addProperty("batch_scan_supported", sql.batchScanSupported());
        output.addProperty("exact_lookup_supported", sql.exactLookupSupported());
        JsonArray required = new JsonArray();
        for (String field : sql.requiredLookupFields()) required.add(field);
        output.add("required_lookup_fields", required);
        addNullable(output, "unavailable_reason", sql.unavailableReason());
        addNullable(output, "note", sql.note());
        return output;
    }

    private static JsonArray rows(List<InspectRow> rows) {
        JsonArray output = new JsonArray();
        for (InspectRow row : rows) output.add(row(row));
        return output;
    }

    private static JsonObject row(InspectRow row) {
        JsonObject output = new JsonObject();
        output.addProperty("bucket", row.bucket());
        output.addProperty("key_b64", encode(row.key()));
        output.addProperty("found", row.found());
        addNullable(output, "value_b64", encode(row.value()));
        JsonArray columns = new JsonArray();
        for (RawBytes column : row.columns()) columns.add(encode(column));
        output.add("columns_b64", columns);
        output.add("decoded_key", decoded(row.decodedKey()));
        output.add("decoded_value", decoded(row.decodedValue()));
        JsonArray decodedColumns = new JsonArray();
        for (DecodedValue value : row.decodedColumns()) decodedColumns.add(decoded(value));
        output.add("decoded_columns", decodedColumns);
        JsonObject parts = new JsonObject();
        for (Map.Entry<String, DecodedValue> part : row.decodedParts().entrySet()) {
            parts.add(part.getKey(), decoded(part.getValue()));
        }
        output.add("decoded_parts", parts);
        JsonArray issues = new JsonArray();
        for (DecodeIssue issue : row.decodeIssues()) {
            JsonObject value = new JsonObject();
            value.addProperty("kind", issue.kind().name());
            addNullable(value, "part", issue.part());
            value.addProperty("message", issue.message());
            issues.add(value);
        }
        output.add("decode_issues", issues);
        return output;
    }

    private static JsonElement decoded(DecodedValue value) {
        if (value == null) return JsonNull.INSTANCE;
        JsonObject output = new JsonObject();
        output.addProperty("kind", value.kind().name());
        addNullable(output, "logical_type", value.logicalType());
        if (value.scalar() != null) output.add("value", scalar(value.scalar()));
        addNullable(output, "raw_b64", encode(value.raw()));
        JsonArray fields = new JsonArray();
        for (DecodedValue.DecodedField field : value.fields()) {
            JsonObject item = new JsonObject();
            item.addProperty("name", field.name());
            item.add("value", decoded(field.value()));
            fields.add(item);
        }
        output.add("fields", fields);
        JsonArray elements = new JsonArray();
        for (DecodedValue element : value.elements()) elements.add(decoded(element));
        output.add("elements", elements);
        JsonArray entries = new JsonArray();
        for (DecodedValue.MapEntry entry : value.entries()) {
            JsonObject item = new JsonObject();
            item.add("key", decoded(entry.key()));
            item.add("value", decoded(entry.value()));
            entries.add(item);
        }
        output.add("entries", entries);
        return output;
    }

    private static JsonElement scalar(Object value) {
        if (value instanceof Boolean) return new JsonPrimitive((Boolean) value);
        if (value instanceof Number) return new JsonPrimitive((Number) value);
        if (value instanceof Character) return new JsonPrimitive((Character) value);
        return new JsonPrimitive(String.valueOf(value));
    }

    private static ScanFilter scanFilter(JsonObject filter) {
        String kind = requiredString(filter, "kind").toLowerCase(java.util.Locale.ROOT);
        if ("sink".equals(kind)) return ScanFilter.sink(fields(array(filter, "fields")));
        if ("state".equals(kind)) {
            return ScanFilter.state(
                    stateKey(object(filter, "state_key")),
                    optionalBoolean(filter, "auto_key_group", false));
        }
        throw invalid("filter kind must be sink or state");
    }

    private static StateKey stateKey(JsonObject value) {
        return new StateKey(
                optionalFields(value, "state_key_fields"),
                optionalFields(value, "namespace_fields"),
                optionalFields(value, "map_key_fields"));
    }

    private static List<FieldValue> optionalFields(JsonObject object, String name) {
        return object.has(name) ? fields(array(object, name)) : Collections.<FieldValue>emptyList();
    }

    private static List<FieldValue> fields(JsonArray values) {
        List<FieldValue> output = new ArrayList<>();
        for (JsonElement element : values) {
            JsonObject field = requireObject(element, "field");
            output.add(
                    new FieldValue(requiredString(field, "name"), typed(object(field, "value"))));
        }
        return output;
    }

    private static TypedValue typed(JsonObject value) {
        String kind = requiredString(value, "kind").toUpperCase(java.util.Locale.ROOT);
        JsonElement raw = value.get("value");
        if (raw == null || raw.isJsonNull()) throw invalid("typed value must not be null");
        final TypedValue.Kind typedKind;
        try {
            typedKind = TypedValue.Kind.valueOf(kind);
        } catch (IllegalArgumentException error) {
            throw invalid("invalid typed value kind");
        }
        switch (typedKind) {
            case STRING:
                return TypedValue.string(strictString(raw, "typed STRING value"));
            case BOOLEAN:
                if (!raw.isJsonPrimitive() || !raw.getAsJsonPrimitive().isBoolean()) {
                    throw invalid("typed BOOLEAN value must be a JSON boolean");
                }
                return TypedValue.bool(raw.getAsBoolean());
            case INTEGER:
                return TypedValue.integer(exactTypedLong(raw));
            case FLOAT:
                if (!raw.isJsonPrimitive() || !raw.getAsJsonPrimitive().isNumber()) {
                    throw invalid("typed FLOAT value must be a JSON number");
                }
                double floating = raw.getAsDouble();
                if (!Double.isFinite(floating)) {
                    throw invalid("typed FLOAT value must be finite");
                }
                return TypedValue.floating(floating);
            case DECIMAL:
                return TypedValue.decimal(strictString(raw, "typed DECIMAL value"));
            case BYTES:
                return TypedValue.bytes(
                        decodeBase64(strictString(raw, "typed BYTES value"), "typed bytes value"));
            case DATE:
                return TypedValue.date(strictString(raw, "typed DATE value"));
            case TIME:
                return TypedValue.time(strictString(raw, "typed TIME value"));
            case TIMESTAMP:
                return TypedValue.timestamp(strictString(raw, "typed TIMESTAMP value"));
            default:
                throw invalid("unsupported typed value kind");
        }
    }

    private static String encode(RawBytes bytes) {
        return bytes == null || bytes.value() == null
                ? null
                : Base64.getEncoder().encodeToString(bytes.value());
    }

    private static byte[] decodeBase64(String value, String field) {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException error) {
            throw invalid(field + " must be valid base64");
        }
    }

    private static JsonObject object(JsonObject parent, String name) {
        JsonElement value = parent.get(name);
        return requireObject(value, name);
    }

    private static JsonObject requireObject(JsonElement value, String name) {
        if (value == null || !value.isJsonObject()) throw invalid(name + " must be an object");
        return value.getAsJsonObject();
    }

    private static JsonArray array(JsonObject parent, String name) {
        JsonElement value = parent.get(name);
        if (value == null || !value.isJsonArray()) throw invalid(name + " must be an array");
        return value.getAsJsonArray();
    }

    private static String requiredString(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull() || !value.isJsonPrimitive()) {
            throw invalid(name + " is required");
        }
        if (!value.getAsJsonPrimitive().isString()) throw invalid(name + " must be a string");
        String text = value.getAsString();
        if (text.trim().isEmpty()) throw invalid(name + " must not be empty");
        return text;
    }

    private static String optionalString(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null || value.isJsonNull() ? null : requiredString(object, name);
    }

    private static int requiredInt(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null
                || value.isJsonNull()
                || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isNumber()) {
            throw invalid(name + " must be an integer");
        }
        try {
            return new BigDecimal(value.getAsString()).intValueExact();
        } catch (ArithmeticException | NumberFormatException error) {
            throw invalid(name + " must be an integer");
        }
    }

    private static int optionalInt(JsonObject object, String name, int fallback) {
        return object.has(name) ? requiredInt(object, name) : fallback;
    }

    private static Integer optionalInteger(JsonObject object, String name) {
        return object.has(name) && !object.get(name).isJsonNull()
                ? Integer.valueOf(requiredInt(object, name))
                : null;
    }

    private static boolean optionalBoolean(JsonObject object, String name, boolean fallback) {
        if (!object.has(name)) return fallback;
        JsonElement value = object.get(name);
        if (value == null
                || value.isJsonNull()
                || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isBoolean()) {
            throw invalid(name + " must be a boolean");
        }
        return value.getAsBoolean();
    }

    private static int[] optionalIntArray(JsonObject object, String name) {
        if (!object.has(name) || object.get(name).isJsonNull()) return null;
        JsonArray values = array(object, name);
        int[] output = new int[values.size()];
        for (int index = 0; index < values.size(); index++) {
            output[index] = exactInt(values.get(index), name + " entries");
        }
        return output;
    }

    private static long requiredNonNegativeLong(JsonElement value, String name) {
        if (value == null
                || value.isJsonNull()
                || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isNumber()) {
            throw invalid(name + " must be 'latest' or a non-negative integer");
        }
        long output = exactLong(value, name);
        if (output < 0) throw invalid(name + " must not be negative");
        return output;
    }

    private static int exactInt(JsonElement value, String name) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw invalid(name + " must be integers");
        }
        try {
            return new BigDecimal(value.getAsString()).intValueExact();
        } catch (ArithmeticException | NumberFormatException error) {
            throw invalid(name + " must be integers");
        }
    }

    private static long exactLong(JsonElement value, String name) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw invalid(name + " must be an integer");
        }
        try {
            return new BigDecimal(value.getAsString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException error) {
            throw invalid(name + " must be an integer");
        }
    }

    private static long exactTypedLong(JsonElement value) {
        if (value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
            String text = value.getAsString();
            if (!text.matches("-?(0|[1-9][0-9]*)")) {
                throw invalid("typed INTEGER value must be a base-10 integer");
            }
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException error) {
                throw invalid("typed INTEGER value is outside the 64-bit range");
            }
        }
        return exactLong(value, "typed INTEGER value");
    }

    private static String strictString(JsonElement value, String name) {
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw invalid(name + " must be a JSON string");
        }
        return value.getAsString();
    }

    private static void addNullable(JsonObject object, String name, String value) {
        if (value == null) object.add(name, JsonNull.INSTANCE);
        else object.addProperty(name, value);
    }

    static InputException invalid(String message) {
        return new InputException(message);
    }
}
