package io.cobble.flink.monitor;

import io.cobble.flink.inspect.internal.DisplayLong;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

final class MonitorTestJson {
    private static final Gson GSON =
            new GsonBuilder()
                    .registerTypeAdapter(
                            DisplayLong.class,
                            (JsonSerializer<DisplayLong>)
                                    (value, type, context) -> new JsonPrimitive(value.toString()))
                    .create();

    private MonitorTestJson() {}

    static String toJson(Object value) {
        return GSON.toJson(value);
    }

    static Map<String, Object> bytesJson(byte[] bytes) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("b64", bytes == null ? null : Base64.getEncoder().encodeToString(bytes));
        value.put("utf8", utf8(bytes));
        return value;
    }

    private static String utf8(byte[] bytes) {
        if (bytes == null) return null;
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException error) {
            return null;
        }
    }
}
