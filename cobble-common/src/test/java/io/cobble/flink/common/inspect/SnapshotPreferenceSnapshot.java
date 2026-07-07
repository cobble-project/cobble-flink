package io.cobble.flink.common.inspect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link TypeSerializerSnapshot} whose {@link #restoreSerializer()} returns {@link
 * StringSerializer#INSTANCE} rather than the original serializer. Used by {@link
 * SerializerInspectSchemaCapturePolicyTest} to prove that {@code restoreSerializer} prefers the
 * snapshot path over the serialized fallback.
 *
 * <p>This is a top-level class (not an inner class) because Flink's {@code
 * InstantiationUtil.resolveClassByName} uses {@code Class.forName} to resolve the snapshot class by
 * name from the serialized bytes, and inner classes may not be resolvable in all classloader
 * configurations.
 */
public class SnapshotPreferenceSnapshot implements TypeSerializerSnapshot<String> {

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {}

    @Override
    public TypeSerializer<String> restoreSerializer() {
        // Restore returns StringSerializer, NOT the original SnapshotPreferenceSerializer.
        return StringSerializer.INSTANCE;
    }

    @Override
    public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(
            TypeSerializer<String> newSerializer) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
}
