package io.cobble.flink.common.inspect;

import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

/** Single-source exact-lookup eligibility for semantic state keys. */
public final class StateInspectExactLookupSupport {
    private static final String VOID_NAMESPACE_SERIALIZER =
            "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    private StateInspectExactLookupSupport() {}

    public static Result evaluate(StateInspectSchema schema, StateInspectSemanticSchema semantic) {
        if (schema == null || semantic == null) {
            return Result.unsupported("state semantic schema is unavailable");
        }
        if (schema.stateKind() == StateKind.LIST || schema.stateKind() == StateKind.TIMER) {
            return Result.unsupported(
                    schema.stateKind() + " state does not support typed exact lookup");
        }
        Result result = check("state key", schema.keySerializer(), semantic.stateKey());
        if (!result.supported()) {
            return result;
        }
        if (!isVoidNamespace(schema.namespaceSerializer())) {
            result = check("namespace", schema.namespaceSerializer(), semantic.namespace());
            if (!result.supported()) {
                return result;
            }
        }
        return schema.stateKind() == StateKind.MAP
                ? check("map key", schema.mapUserKeySerializer(), semantic.mapUserKey())
                : Result.allowed();
    }

    private static Result check(
            String label, SerializerInspectSchema serializer, StateInspectType type) {
        if (type == null || type.kind() == StateInspectTypeKind.UNKNOWN) {
            return Result.unsupported(label + " semantic type is unavailable");
        }
        if (type.kind() == StateInspectTypeKind.TUPLE) {
            return Result.unsupported(label + " Tuple reconstruction is not supported");
        }
        if (type.kind() != StateInspectTypeKind.SCALAR && type.kind() != StateInspectTypeKind.ROW) {
            return Result.unsupported(
                    label + " requires a scalar or flat scalar ROW for exact lookup");
        }
        if (type.kind() == StateInspectTypeKind.SCALAR && !isScalar(type)) {
            return Result.unsupported(label + " has a non-scalar logical type");
        }
        if (type.kind() == StateInspectTypeKind.ROW) {
            for (StateInspectField field : type.fields()) {
                if (!isScalar(field.type())) {
                    return Result.unsupported(label + " contains a nested non-scalar field");
                }
            }
        }
        InspectDecoderDescriptor descriptor =
                serializer == null ? null : serializer.decoderDescriptor();
        if (type.kind() == StateInspectTypeKind.ROW
                && descriptor != null
                && (descriptor.isPojo() || descriptor.isAvro())) {
            return Result.unsupported(
                    "classless " + (descriptor.isPojo() ? "POJO" : "Avro") + " " + label);
        }
        return Result.allowed();
    }

    private static boolean isScalar(StateInspectType type) {
        if (type.kind() != StateInspectTypeKind.SCALAR) return false;
        try {
            return LogicalTypeParser.parse(
                            type.logicalType(),
                            StateInspectExactLookupSupport.class.getClassLoader())
                    .getChildren()
                    .isEmpty();
        } catch (RuntimeException unsupported) {
            return false;
        }
    }

    private static boolean isVoidNamespace(SerializerInspectSchema serializer) {
        return serializer != null
                && VOID_NAMESPACE_SERIALIZER.equals(serializer.serializerClassName());
    }

    public static final class Result {
        private final boolean supported;
        private final String reason;

        private Result(boolean supported, String reason) {
            this.supported = supported;
            this.reason = reason;
        }

        private static Result allowed() {
            return new Result(true, null);
        }

        private static Result unsupported(String reason) {
            return new Result(false, reason);
        }

        public boolean supported() {
            return supported;
        }

        public String reason() {
            return reason;
        }
    }
}
