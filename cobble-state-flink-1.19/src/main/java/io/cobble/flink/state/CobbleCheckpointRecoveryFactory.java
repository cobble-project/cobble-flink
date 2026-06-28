package io.cobble.flink.state;

import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** Wraps Flink checkpoint recovery so completed checkpoints also drive Cobble global snapshots. */
final class CobbleCheckpointRecoveryFactory implements InvocationHandler {

    private static final String RECOVERED_STORE_METHOD = "createRecoveredCompletedCheckpointStore";

    private final CheckpointRecoveryFactory delegate;

    private CobbleCheckpointRecoveryFactory(CheckpointRecoveryFactory delegate) {
        this.delegate = delegate;
    }

    static CheckpointRecoveryFactory wrap(CheckpointRecoveryFactory delegate) {
        return (CheckpointRecoveryFactory)
                Proxy.newProxyInstance(
                        delegate.getClass().getClassLoader(),
                        new Class<?>[] {CheckpointRecoveryFactory.class},
                        new CobbleCheckpointRecoveryFactory(delegate));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return invokeObjectMethod(proxy, method, args);
        }
        try {
            Object result = method.invoke(delegate, args);
            if (RECOVERED_STORE_METHOD.equals(method.getName())
                    && method.getParameterCount() == 5) {
                return new CobbleCompletedCheckpointStore(
                        (org.apache.flink.runtime.checkpoint.CompletedCheckpointStore) result);
            }
            return result;
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
        switch (method.getName()) {
            case "toString":
                return "CobbleCheckpointRecoveryFactory{" + delegate + '}';
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new UnsupportedOperationException("Unsupported Object method: " + method);
        }
    }
}
