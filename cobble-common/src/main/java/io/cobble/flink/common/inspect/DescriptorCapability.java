package io.cobble.flink.common.inspect;

/**
 * Declares whether an {@link InspectDecoderDescriptor} can decode values without user classes.
 *
 * <ul>
 *   <li>{@code FULLY_CLASSLESS} - every row can be decoded from the descriptor alone. The monitor
 *       must not persist a serialized live serializer fallback for this part.
 *   <li>{@code PARTIALLY_CLASSLESS} - some rows can be decoded (e.g. base POJO without subclass
 *       flag); others need the snapshot-restored serializer or {@code --user-jar} fallback. The
 *       monitor must keep the serialized live serializer for the unsupported rows.
 *   <li>{@code UNSUPPORTED} - the descriptor cannot decode any row; fall back entirely.
 * </ul>
 *
 * <p>Enum ordinals are persisted in the inspect schema store. The store format is not
 * backward-compatible across releases (see {@link StateInspectSchemaStore} version).
 */
public enum DescriptorCapability {
    FULLY_CLASSLESS,
    PARTIALLY_CLASSLESS,
    UNSUPPORTED
}
