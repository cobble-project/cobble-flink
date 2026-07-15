package io.cobble.flink.inspect.internal;

import java.util.function.Supplier;

/** Runs serializer restoration with a bounded thread-context-classloader scope. */
public final class UserClassLoaderScope {
    private UserClassLoaderScope() {}

    public static <T> T call(ClassLoader classLoader, Supplier<T> action) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(classLoader);
            return action.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
