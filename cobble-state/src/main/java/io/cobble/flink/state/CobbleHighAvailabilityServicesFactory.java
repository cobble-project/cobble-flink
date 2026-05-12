package io.cobble.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Locale;
import java.util.concurrent.Executor;

/**
 * HA factory that wraps an inner Flink HA implementation and adds Cobble checkpoint side effects.
 *
 * <p>The tricky case is {@code cobble.ha.delegate.type=NONE}. In Flink 1.17 there is no stable SPI
 * signal that tells a custom {@link HighAvailabilityServicesFactory} whether it is being created
 * from an in-process {@code MiniCluster} or from the standalone multi-process daemons. The choice
 * matters because:
 *
 * <ul>
 *   <li>standalone daemons need {@link
 *       HighAvailabilityServicesUtils#createHighAvailabilityServices} so TaskManagers can resolve
 *       the JobManager/ResourceManager leader through configured RPC addresses, while
 *   <li>MiniCluster-style embedded execution needs {@link
 *       HighAvailabilityServicesUtils#createAvailableOrEmbeddedServices} so all components share
 *       one in-JVM embedded HA service instead of trying to talk to a standalone RPC endpoint.
 * </ul>
 *
 * <p>To make the behavior explicit and debuggable, Cobble exposes {@code
 * cobble.ha.delegate.none-mode} with three values:
 *
 * <ul>
 *   <li>{@code standalone}: never guess; always use standalone NONE-mode behavior.
 *   <li>{@code embedded}: never guess; always use embedded NONE-mode behavior.
 *   <li>{@code auto}: inspect the current Java call stack for the specific Flink HA utility method
 *       that is creating this factory instance. If the stack shows {@code
 *       HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(...)} then Cobble chooses
 *       embedded mode. If it shows {@code
 *       HighAvailabilityServicesUtils.createHighAvailabilityServices(...)} then Cobble chooses
 *       standalone mode.
 * </ul>
 *
 * <p>{@code auto} is still a heuristic because Flink does not expose a first-class SPI flag for
 * "this custom HA factory is being created for MiniCluster/embedded execution". However, matching
 * the exact HA utility branch is less fragile than checking unrelated configuration side effects
 * such as random ports, and it keeps the guess aligned with Flink's own control flow.
 */
public final class CobbleHighAvailabilityServicesFactory
        implements HighAvailabilityServicesFactory {

    private static final String HA_UTILS_CLASS =
            "org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils";
    private static final String AVAILABLE_OR_EMBEDDED_METHOD = "createAvailableOrEmbeddedServices";
    private static final String STANDALONE_METHOD = "createHighAvailabilityServices";

    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor)
            throws Exception {
        Configuration delegateConfiguration = createDelegateConfiguration(configuration);
        DelegateNoneMode noneMode = DelegateNoneMode.fromConfiguration(configuration);
        HighAvailabilityServices delegate =
                shouldUseEmbeddedDelegate(delegateConfiguration, noneMode)
                        ? HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
                                delegateConfiguration,
                                executor,
                                fatalErrorHandler("Cobble delegate HA service failed."))
                        : HighAvailabilityServicesUtils.createHighAvailabilityServices(
                                delegateConfiguration,
                                executor,
                                AddressResolution.NO_ADDRESS_RESOLUTION,
                                RpcSystem.load(configuration),
                                fatalErrorHandler("Cobble delegate HA service failed."));
        return new CobbleHighAvailabilityServices(delegate);
    }

    @Override
    public ClientHighAvailabilityServices createClientHAServices(Configuration configuration)
            throws Exception {
        // Client HA resolution does not have the same embedded-versus-standalone ambiguity as the
        // server-side createHAServices() path. Flink's client utility for HA_MODE=NONE is already
        // the REST/web-monitor based client view, so we delegate through the standard client path.
        return HighAvailabilityServicesUtils.createClientHAService(
                createDelegateConfiguration(configuration),
                fatalErrorHandler("Cobble delegate client HA service failed."));
    }

    private static Configuration createDelegateConfiguration(Configuration configuration) {
        String delegateType =
                configuration.getString(CobbleHighAvailabilityOptions.DELEGATE_HA_TYPE);
        if (delegateType == null || delegateType.trim().isEmpty()) {
            throw new IllegalConfigurationException(
                    "Missing required Cobble HA delegate type: "
                            + CobbleHighAvailabilityOptions.DELEGATE_HA_TYPE.key());
        }
        if (CobbleHighAvailabilityServicesFactory.class.getName().equals(delegateType.trim())) {
            throw new IllegalConfigurationException(
                    "Cobble HA delegate type must not point back to "
                            + CobbleHighAvailabilityServicesFactory.class.getName()
                            + '.');
        }

        Configuration delegateConfiguration = new Configuration(configuration);
        delegateConfiguration.setString(HighAvailabilityOptions.HA_MODE, delegateType.trim());
        return delegateConfiguration;
    }

    private static boolean shouldUseEmbeddedDelegate(
            Configuration configuration, DelegateNoneMode noneMode) {
        if (HighAvailabilityMode.fromConfig(configuration) != HighAvailabilityMode.NONE) {
            return false;
        }
        switch (noneMode) {
            case EMBEDDED:
                return true;
            case STANDALONE:
                return false;
            case AUTO:
                return isEmbeddedHaCreationStack(Thread.currentThread().getStackTrace());
            default:
                throw new IllegalStateException("Unexpected NONE-mode branch: " + noneMode);
        }
    }

    static boolean isEmbeddedHaCreationStack(StackTraceElement[] stackTrace) {
        if (containsHaUtilsMethod(stackTrace, AVAILABLE_OR_EMBEDDED_METHOD)) {
            return true;
        }
        if (containsHaUtilsMethod(stackTrace, STANDALONE_METHOD)) {
            return false;
        }
        // Fall back to standalone when the stack shape is unknown. Standalone is the safer default
        // for external daemons because it preserves RPC-based leader discovery instead of assuming
        // an in-JVM embedded HA singleton that may not exist.
        return false;
    }

    private static boolean containsHaUtilsMethod(
            StackTraceElement[] stackTrace, String expectedMethodName) {
        for (StackTraceElement element : stackTrace) {
            if (HA_UTILS_CLASS.equals(element.getClassName())
                    && expectedMethodName.equals(element.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    private static FatalErrorHandler fatalErrorHandler(String message) {
        return throwable -> {
            throw new FlinkRuntimeException(message, throwable);
        };
    }

    private enum DelegateNoneMode {
        AUTO,
        EMBEDDED,
        STANDALONE;

        private static DelegateNoneMode fromConfiguration(Configuration configuration) {
            String rawValue =
                    configuration.getString(CobbleHighAvailabilityOptions.DELEGATE_NONE_MODE);
            try {
                return DelegateNoneMode.valueOf(rawValue.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalConfigurationException(
                        "Unsupported Cobble HA delegate NONE mode '"
                                + rawValue
                                + "'. Expected one of: auto, embedded, standalone.",
                        e);
            }
        }
    }
}
