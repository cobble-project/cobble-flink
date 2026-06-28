package io.cobble.flink.state;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** Delegating HA services that only wraps checkpoint recovery with Cobble global snapshot logic. */
final class CobbleHighAvailabilityServices implements InvocationHandler {

    private static final String CHECKPOINT_RECOVERY_FACTORY_METHOD = "getCheckpointRecoveryFactory";

    private final HighAvailabilityServices delegate;

    private CobbleHighAvailabilityServices(HighAvailabilityServices delegate) {
        this.delegate = delegate;
    }

    static HighAvailabilityServices wrap(HighAvailabilityServices delegate) {
        return (HighAvailabilityServices)
                Proxy.newProxyInstance(
                        delegate.getClass().getClassLoader(),
                        new Class<?>[] {HighAvailabilityServices.class},
                        new CobbleHighAvailabilityServices(delegate));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return invokeObjectMethod(proxy, method, args);
        }
        if (CHECKPOINT_RECOVERY_FACTORY_METHOD.equals(method.getName())
                && method.getParameterCount() == 0) {
            return CobbleCheckpointRecoveryFactory.wrap(delegate.getCheckpointRecoveryFactory());
        }
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
        switch (method.getName()) {
            case "toString":
                return "CobbleHighAvailabilityServices{" + delegate + '}';
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new UnsupportedOperationException("Unsupported Object method: " + method);
        }
    }
}
