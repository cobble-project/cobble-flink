package io.cobble.flink.monitor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.lang.reflect.Method;

/** Keeps shared monitor fixtures compatible with the supported Flink API lines. */
final class FlinkTestSupport {
    private FlinkTestSupport() {}

    @SuppressWarnings("unchecked")
    static <T> TypeSerializer<T> createSerializer(TypeInformation<T> type, ExecutionConfig config) {
        try {
            Method serializerConfig;
            try {
                serializerConfig = ExecutionConfig.class.getMethod("getSerializerConfig");
            } catch (NoSuchMethodException legacy) {
                return (TypeSerializer<T>)
                        TypeInformation.class
                                .getMethod("createSerializer", ExecutionConfig.class)
                                .invoke(type, config);
            }
            return (TypeSerializer<T>)
                    TypeInformation.class
                            .getMethod("createSerializer", serializerConfig.getReturnType())
                            .invoke(type, serializerConfig.invoke(config));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create Flink test serializer.", e);
        }
    }

    static void registerPojoType(ExecutionConfig config, Class<?> type) {
        try {
            Object target;
            try {
                target = ExecutionConfig.class.getMethod("getSerializerConfig").invoke(config);
            } catch (NoSuchMethodException legacy) {
                target = config;
            }
            target.getClass().getMethod("registerPojoType", Class.class).invoke(target, type);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to register Flink test POJO.", e);
        }
    }

    static OperatorState operatorState(OperatorID id, int parallelism, int maxParallelism) {
        try {
            try {
                return OperatorState.class
                        .getConstructor(
                                String.class, String.class, OperatorID.class, int.class, int.class)
                        .newInstance(
                                "monitor-test", "monitor-test", id, parallelism, maxParallelism);
            } catch (NoSuchMethodException legacy) {
                return OperatorState.class
                        .getConstructor(OperatorID.class, int.class, int.class)
                        .newInstance(id, parallelism, maxParallelism);
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create Flink test operator state.", e);
        }
    }

    static MetadataSerializer metadataSerializer() {
        int version = Integer.getInteger("monitor.metadata.version", 4);
        try {
            return (MetadataSerializer)
                    Class.forName(
                                    "org.apache.flink.runtime.checkpoint.metadata.MetadataV"
                                            + version
                                            + "Serializer")
                            .getField("INSTANCE")
                            .get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Missing checkpoint metadata serializer v" + version, e);
        }
    }
}
