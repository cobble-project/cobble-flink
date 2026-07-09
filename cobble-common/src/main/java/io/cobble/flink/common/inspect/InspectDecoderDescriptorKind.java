package io.cobble.flink.common.inspect;

/**
 * Kind tag for {@link InspectDecoderDescriptor}. Identifies which classless decode strategy the
 * monitor should use for a given serializer snapshot.
 *
 * <p>Enum ordinals are persisted in the inspect schema store. The store format is not
 * backward-compatible across releases (see {@link StateInspectSchemaStore} version), so ordinals
 * need not be frozen indefinitely.
 *
 * <ul>
 *   <li>{@code PORTABLE_SNAPSHOT} - the monitor can restore a Flink built-in serializer from the
 *       persisted snapshot bytes. Covers primitives, Tuple/List/Map trees, and portable RowData.
 *   <li>{@code POJO} - a hand-written wire decoder for Flink's {@code PojoSerializer} flag/field
 *       protocol, using only field names and child descriptors (no POJO class loading).
 *   <li>{@code AVRO} - a hand-written wire decoder using Flink's {@code DataInputDecoder} semantics
 *       and the persisted Avro writer schema JSON (no AvroSerializer or SpecificRecord class
 *       required).
 *   <li>{@code UNSUPPORTED} - no classless decode path; the monitor must fall back to
 *       snapshot-restored serializer, {@code --user-jar}, or raw bytes.
 * </ul>
 */
public enum InspectDecoderDescriptorKind {
    PORTABLE_SNAPSHOT,
    POJO,
    AVRO,
    UNSUPPORTED
}
