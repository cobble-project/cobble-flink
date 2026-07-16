package io.cobble.flink.inspect.internal;

import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/** Classifies only failures that clearly identify a missing checkpoint file or path. */
final class CheckpointUnavailableClassifier {
    private static final int MAX_CAUSE_DEPTH = 32;
    private static final Pattern MISSING_FILE_SIGNAL =
            Pattern.compile(
                    "(?:no such file(?: or directory)?|no such (?:key|path)|nosuchkey|"
                            + "the specified key does not exist|file not found|"
                            + "os error 2|(?:http|status)[^\\n]{0,48}\\b404\\b|"
                            + "(?:file|path|directory|manifest|snapshot|checkpoint)"
                            + "[^\\n]{0,160}(?:does not exist|not found|is missing))",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern NON_MISSING_SIGNAL =
            Pattern.compile(
                    "(?:permission denied|access denied|unauthorized|forbidden|"
                            + "unknown column family|serializer|deserializ|checksum|corrupt)",
                    Pattern.CASE_INSENSITIVE);

    private CheckpointUnavailableClassifier() {}

    static boolean isCheckpointUnavailable(Throwable failure) {
        Set<Throwable> visited =
                Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        StringBuilder messages = new StringBuilder();
        boolean missingFileType = false;
        Throwable current = failure;
        int depth = 0;
        while (current != null && depth++ < MAX_CAUSE_DEPTH && visited.add(current)) {
            if (current instanceof FileNotFoundException
                    || current instanceof NoSuchFileException) {
                missingFileType = true;
            }
            if (current.getMessage() != null) {
                messages.append(current.getMessage().toLowerCase(Locale.ROOT)).append('\n');
            }
            current = current.getCause();
        }
        String combined = messages.toString();
        return !NON_MISSING_SIGNAL.matcher(combined).find()
                && (missingFileType || MISSING_FILE_SIGNAL.matcher(combined).find());
    }
}
