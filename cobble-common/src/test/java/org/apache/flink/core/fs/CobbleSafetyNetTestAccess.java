package org.apache.flink.core.fs;

/** Exposes the package-private registry constructor for focused wrapper contract tests. */
public final class CobbleSafetyNetTestAccess {
    private CobbleSafetyNetTestAccess() {}

    public static SafetyNetCloseableRegistry newRegistry() {
        return new SafetyNetCloseableRegistry();
    }
}
