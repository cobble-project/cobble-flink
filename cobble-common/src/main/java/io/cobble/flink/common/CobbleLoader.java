package io.cobble.flink.common;

import io.cobble.NativeLoader;

/** Utility to load Cobble JNI and register Flink filesystem fallback. */
public class CobbleLoader {
    /** Loads Cobble JNI once and registers Flink filesystem fallback once per process. */
    public static void ensureCobbleLoaded() {
        NativeLoader.load();
        CobbleFlinkFileSystems.ensureRegistered();
    }

    private CobbleLoader() {}
}
