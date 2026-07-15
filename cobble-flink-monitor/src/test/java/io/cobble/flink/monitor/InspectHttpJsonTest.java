package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.flink.inspect.DecodedValue;
import io.cobble.flink.inspect.InspectPage;
import io.cobble.flink.inspect.InspectRow;
import io.cobble.flink.inspect.LookupRequest;
import io.cobble.flink.inspect.RawBytes;
import io.cobble.flink.inspect.internal.DisplayLong;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class InspectHttpJsonTest {
    @Test
    void numericTransportFieldsRequireJsonIntegersWithoutCoercion() {
        assertInvalid(
                () ->
                        InspectHttpJson.scanRequest(
                                json("{\"target_id\":\"raw\",\"limit\":1.5}"), "session", 10, 100));
        assertInvalid(
                () ->
                        InspectHttpJson.scanRequest(
                                json("{\"target_id\":\"raw\",\"bucket\":\"12\"}"),
                                "session",
                                10,
                                100));
        assertInvalid(
                () ->
                        InspectHttpJson.selection(
                                json("{\"source\":\"file:///tmp\",\"checkpoint\":\"12\"}")));
        assertInvalid(
                () ->
                        InspectHttpJson.selection(
                                json("{\"source\":\"file:///tmp\",\"checkpoint\":1.5}")));
        assertInvalid(
                () -> InspectHttpJson.selection(json("{\"source\":12,\"checkpoint\":\"latest\"}")));
    }

    @Test
    void booleanTransportFieldsRequireJsonBoolean() {
        JsonObject body =
                json(
                        "{\"target_id\":\"state\",\"filter\":{\"kind\":\"state\","
                                + "\"auto_key_group\":\"true\",\"state_key\":{"
                                + "\"state_key_fields\":[]}}}");
        assertInvalid(() -> InspectHttpJson.scanRequest(body, "session", 10, 100));

        JsonObject typed =
                json(
                        "{\"target_id\":\"sink\",\"keys\":[{\"kind\":\"sink\","
                                + "\"fields\":[{\"name\":\"enabled\",\"value\":{"
                                + "\"kind\":\"BOOLEAN\",\"value\":\"true\"}}]}]}");
        assertInvalid(() -> InspectHttpJson.lookupRequest(typed, 10));
    }

    @Test
    void integerRangeOverflowIsRejected() {
        assertInvalid(
                () ->
                        InspectHttpJson.scanRequest(
                                json("{\"target_id\":\"raw\",\"limit\":2147483648}"),
                                "session",
                                10,
                                Integer.MAX_VALUE));
        assertInvalid(
                () ->
                        InspectHttpJson.selection(
                                json(
                                        "{\"source\":\"file:///tmp\","
                                                + "\"checkpoint\":9223372036854775808}")));
    }

    @Test
    void typedIntegerAcceptsExactLongStringWithoutJavascriptPrecisionLoss() {
        LookupRequest request =
                InspectHttpJson.lookupRequest(
                        json(
                                "{\"target_id\":\"sink\",\"keys\":[{\"kind\":\"sink\","
                                        + "\"fields\":[{\"name\":\"id\",\"value\":{"
                                        + "\"kind\":\"INTEGER\",\"value\":\"9223372036854775807\"}}]}]}"),
                        10);

        assertEquals(
                "9223372036854775807",
                request.keys().get(0).typedKey().sinkKeyFields().get(0).value().text());
    }

    @Test
    void sourceUriRejectsEmbeddedCredentialsAndQueryParameters() {
        assertInvalid(
                () ->
                        InspectHttpJson.selection(
                                json(
                                        "{\"source\":\"s3://access:secret@example/bucket\","
                                                + "\"checkpoint\":\"latest\"}")));
        assertInvalid(
                () ->
                        InspectHttpJson.selection(
                                json(
                                        "{\"source\":\"s3://example/bucket?token=secret\","
                                                + "\"checkpoint\":\"latest\"}")));
    }

    @Test
    void decodedLongUsesAJsonStringAtTheHttpBoundary() {
        DecodedValue decoded =
                new DecodedValue(
                        DecodedValue.Kind.SCALAR,
                        "BIGINT",
                        DisplayLong.forJson(Long.MAX_VALUE),
                        null,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        InspectRow row =
                new InspectRow(
                        0,
                        new RawBytes(new byte[] {1}),
                        Collections.emptyList(),
                        true,
                        null,
                        decoded,
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyList());

        JsonObject output =
                InspectHttpJson.page(
                        "session", new InspectPage(Collections.singletonList(row), null));

        assertTrue(
                output.getAsJsonArray("rows")
                        .get(0)
                        .getAsJsonObject()
                        .getAsJsonObject("decoded_key")
                        .get("value")
                        .getAsJsonPrimitive()
                        .isString());
    }

    private static JsonObject json(String value) {
        return JsonParser.parseString(value).getAsJsonObject();
    }

    private static void assertInvalid(Runnable runnable) {
        assertThrows(InputException.class, runnable::run);
    }
}
