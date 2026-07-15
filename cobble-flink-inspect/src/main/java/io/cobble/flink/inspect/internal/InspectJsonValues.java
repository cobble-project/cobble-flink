package io.cobble.flink.inspect.internal;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/** Legacy monitor JSON rendering kept below the public SDK boundary. */
final class InspectJsonValues {
    private InspectJsonValues() {}

    static Map<String, Object> bytesJson(byte[] bytes) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("b64", Base64.getEncoder().encodeToString(bytes));
        value.put("utf8", new String(bytes, StandardCharsets.UTF_8));
        return value;
    }
}
