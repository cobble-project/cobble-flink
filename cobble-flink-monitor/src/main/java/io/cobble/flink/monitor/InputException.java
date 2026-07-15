package io.cobble.flink.monitor;

/** HTTP-layer input error retained for the monitor's status-code mapping. */
final class InputException extends RuntimeException {
    InputException(String message) {
        super(message);
    }
}
