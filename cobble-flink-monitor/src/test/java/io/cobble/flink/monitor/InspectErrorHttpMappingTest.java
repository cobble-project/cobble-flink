package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cobble.flink.inspect.InspectErrorCode;
import io.cobble.flink.inspect.InspectException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class InspectErrorHttpMappingTest {

    @Test
    void checkpointUnavailableUsesGoneWithStableWireCode() throws Exception {
        RecordingExchange exchange = new RecordingExchange();

        sendInspectError(
                exchange,
                new InspectException(InspectErrorCode.CHECKPOINT_UNAVAILABLE, "missing manifest"));

        JsonObject body = JsonParser.parseString(exchange.body()).getAsJsonObject();
        assertEquals(410, exchange.status);
        assertEquals("CHECKPOINT_UNAVAILABLE", body.get("code").getAsString());
        assertEquals("The checkpoint is incomplete or expired", body.get("message").getAsString());
    }

    private static void sendInspectError(RecordingExchange exchange, InspectException error)
            throws Exception {
        Method method =
                CobbleFlinkMonitorServer.class.getDeclaredMethod(
                        "sendInspectError",
                        HttpExchange.class,
                        InspectException.class,
                        String.class);
        method.setAccessible(true);
        method.invoke(null, exchange, error, "request-id");
    }

    private static final class RecordingExchange extends HttpExchange {
        private final Headers requestHeaders = new Headers();
        private final Headers responseHeaders = new Headers();
        private final ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
        private final Map<String, Object> attributes = new HashMap<>();
        private int status = -1;

        @Override
        public Headers getRequestHeaders() {
            return requestHeaders;
        }

        @Override
        public Headers getResponseHeaders() {
            return responseHeaders;
        }

        @Override
        public URI getRequestURI() {
            return URI.create("/api/v1/sessions/session/scan");
        }

        @Override
        public String getRequestMethod() {
            return "POST";
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public InputStream getRequestBody() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public OutputStream getResponseBody() {
            return responseBody;
        }

        @Override
        public void sendResponseHeaders(int status, long contentLength) {
            this.status = status;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return new InetSocketAddress("127.0.0.1", 0);
        }

        @Override
        public int getResponseCode() {
            return status;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return new InetSocketAddress("127.0.0.1", 0);
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return attributes.get(name);
        }

        @Override
        public void setAttribute(String name, Object value) {
            attributes.put(name, value);
        }

        @Override
        public void setStreams(InputStream input, OutputStream output) {}

        @Override
        public HttpPrincipal getPrincipal() {
            return null;
        }

        private String body() {
            return new String(responseBody.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
