package io.cobble.flink.monitor;

import io.cobble.flink.inspect.internal.InspectInputException;

/** HTTP-layer input error retained for the monitor's status-code mapping. */
final class InputException extends InspectInputException {
    InputException(String message) {
        super(message);
    }
}
