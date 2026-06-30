package io.cobble.flink.table;

import io.cobble.flink.common.inspect.StateKind;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Parsed and validated {@code state.*} options for a Cobble state source.
 *
 * <p>This only covers option-shape validation (presence, allowed values). Cross-checks against the
 * resolved inspect schema — operator existence, state name, kind match — happen later in {@link
 * StateSourceSchemaResolver} where the schema registry is available.
 */
final class StateSourceOptions {

    private final String stateName;
    private final String operatorId;
    private final StateKind stateKindHint;

    private StateSourceOptions(String stateName, String operatorId, StateKind stateKindHint) {
        this.stateName = stateName;
        this.operatorId = operatorId;
        this.stateKindHint = stateKindHint;
    }

    /** The required logical state name. */
    String stateName() {
        return stateName;
    }

    /** The optional explicit Cobble operator id, or {@code null} when not set. */
    String operatorId() {
        return operatorId;
    }

    /** The optional {@code state.kind} validation hint, or {@code null} when not set. */
    StateKind stateKindHint() {
        return stateKindHint;
    }

    /**
     * Parses the {@code state.*} options for a state source.
     *
     * @throws ValidationException when {@code state.name} is missing or {@code state.kind} is not a
     *     recognized kind.
     */
    static StateSourceOptions parseForState(ReadableConfig options) {
        String stateName = trimToNull(options.getOptional(CobbleSourceTableOptions.STATE_NAME));
        if (stateName == null) {
            throw new ValidationException(
                    "Option '"
                            + CobbleSourceTableOptions.STATE_NAME.key()
                            + "' is required when source.kind='state'.");
        }
        String operatorId =
                trimToNull(options.getOptional(CobbleSourceTableOptions.STATE_OPERATOR_ID));
        StateKind stateKindHint =
                parseStateKind(
                        trimToNull(options.getOptional(CobbleSourceTableOptions.STATE_KIND)));
        return new StateSourceOptions(stateName, operatorId, stateKindHint);
    }

    /**
     * Rejects {@code state.*} options on a sink source so typos (for example a misspelled {@code
     * source.kind}) surface instead of being silently ignored.
     */
    static void rejectStateOptionsForSink(ReadableConfig options) {
        rejectIfPresent(options, CobbleSourceTableOptions.STATE_NAME);
        rejectIfPresent(options, CobbleSourceTableOptions.STATE_OPERATOR_ID);
        rejectIfPresent(options, CobbleSourceTableOptions.STATE_KIND);
    }

    private static void rejectIfPresent(
            ReadableConfig options, org.apache.flink.configuration.ConfigOption<String> option) {
        if (trimToNull(options.getOptional(option)) != null) {
            throw new ValidationException(
                    "Option '" + option.key() + "' is only valid when source.kind='state'.");
        }
    }

    private static StateKind parseStateKind(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        for (StateKind kind : StateKind.values()) {
            if (kind.wireName().equals(normalized)) {
                return kind;
            }
        }
        throw new ValidationException(
                "Invalid "
                        + CobbleSourceTableOptions.STATE_KIND.key()
                        + " '"
                        + value
                        + "'. Valid values are: "
                        + validStateKinds()
                        + ".");
    }

    private static String validStateKinds() {
        return Arrays.stream(StateKind.values())
                .map(StateKind::wireName)
                .collect(Collectors.joining(", "));
    }

    private static String trimToNull(Optional<String> value) {
        if (!value.isPresent()) {
            return null;
        }
        String trimmed = value.get().trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
