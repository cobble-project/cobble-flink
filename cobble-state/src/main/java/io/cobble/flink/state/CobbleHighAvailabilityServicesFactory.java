package io.cobble.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.Executor;

/**
 * HA factory that wraps an inner Flink HA implementation and adds Cobble checkpoint side effects.
 */
public final class CobbleHighAvailabilityServicesFactory
        implements HighAvailabilityServicesFactory {

    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor)
            throws Exception {
        HighAvailabilityServices delegate =
                HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
                        createDelegateConfiguration(configuration),
                        executor,
                        fatalErrorHandler("Cobble delegate HA service failed."));
        return new CobbleHighAvailabilityServices(delegate);
    }

    @Override
    public ClientHighAvailabilityServices createClientHAServices(Configuration configuration)
            throws Exception {
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

    private static FatalErrorHandler fatalErrorHandler(String message) {
        return throwable -> {
            throw new FlinkRuntimeException(message, throwable);
        };
    }
}
