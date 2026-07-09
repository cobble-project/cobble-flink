package io.cobble.flink.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Browser-side regression checks for the static monitor UI JavaScript. */
class MonitorAppJsTest {

    @Test
    void semanticTableGroupsUsesAccumulatorLabelForAggregatingAndValueForOthers() throws Exception {
        String appJs = readAppJs();
        String harness =
                "const document = {\n"
                        + "  getElementById: () => ({ addEventListener() {}, classList: { toggle() {}, remove() {}, add() {} }, setAttribute() {} }),\n"
                        + "  querySelectorAll: () => [],\n"
                        + "  querySelector: () => ({ classList: { toggle() {} } }),\n"
                        + "  addEventListener() {},\n"
                        + "};\n"
                        + "const window = { addEventListener() {} };\n"
                        + "async function fetch() { return { ok: true, json: async () => ({}) }; }\n"
                        + appJs
                        + "\nconst valueType = { kind: 'SCALAR', logical_type: 'BIGINT' };\n"
                        + "const aggregating = semanticTableGroups({ semantic_parts: { value: valueType }, value_part_label: 'Accumulator' }).find((g) => g.id === 'value').label;\n"
                        + "const aggregatingCamel = semanticTableGroups({ semanticParts: { value: valueType }, valuePartLabel: 'Accumulator' }).find((g) => g.id === 'value').label;\n"
                        + "const value = semanticTableGroups({ semantic_parts: { value: valueType }, state_kind: 'VALUE' }).find((g) => g.id === 'value').label;\n"
                        + "const reducing = semanticTableGroups({ semantic_parts: { value: valueType }, state_kind: 'REDUCING' }).find((g) => g.id === 'value').label;\n"
                        + "const aggregatingTarget = { kind: 'state', semantic_parts: { value: valueType }, value_part_label: 'Accumulator' };\n"
                        + "const reducingTarget = { kind: 'state', semantic_parts: { value: valueType }, state_kind: 'REDUCING' };\n"
                        + "const rendered = renderStateGroupHeaders(semanticTableGroups(aggregatingTarget));\n"
                        + "const matchingSignature = semanticTableSignature(aggregatingTarget) === semanticTableSignature(aggregatingTarget);\n"
                        + "const mixedSignature = semanticTableSignature(aggregatingTarget) === semanticTableSignature(reducingTarget);\n"
                        + "console.log(JSON.stringify({ aggregating, aggregatingCamel, value, reducing, rendered, matchingSignature, mixedSignature }));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        assertEquals(0, exit, stderr);

        assertTrue(stdout.contains("\"aggregating\":\"Accumulator\""));
        assertTrue(stdout.contains("\"aggregatingCamel\":\"Accumulator\""));
        assertTrue(stdout.contains("\"value\":\"Value\""));
        assertTrue(stdout.contains("\"reducing\":\"Value\""));
        assertTrue(stdout.contains("Accumulator"));
        assertFalse(stdout.contains(">Value</th>"));
        assertTrue(stdout.contains("\"matchingSignature\":true"));
        assertTrue(stdout.contains("\"mixedSignature\":false"));
    }

    @Test
    void overviewShowsRunnableSinkAndStateSourceSql() throws Exception {
        String appJs = readAppJs();
        String harness =
                "const document = {\n"
                        + "  getElementById: () => ({ addEventListener() {}, classList: { toggle() {}, remove() {}, add() {} }, setAttribute() {}, querySelectorAll: () => [] }),\n"
                        + "  querySelectorAll: () => [],\n"
                        + "  querySelector: () => ({ classList: { toggle() {} } }),\n"
                        + "  addEventListener() {},\n"
                        + "};\n"
                        + "const window = { addEventListener() {} };\n"
                        + "async function fetch() { return { ok: true, json: async () => ({}) }; }\n"
                        + appJs
                        + "\nconst meta = { source_open: true, source_path: 'file:///tmp/cobble-table', selected_checkpoint: 'latest', selected_checkpoint_id: 9, selected_operator_id: 'op-1', selected_checkpoint_directory: 'file:///tmp/checkpoints/job-1/chk-9' };\n"
                        + "const sinkTarget = { kind: 'sink', key_fields: [{ name: 'id', logical_type: 'BIGINT', row_index: 1 }], value_fields: [{ name: 'name', logical_type: 'VARCHAR(2147483647)', row_index: 0 }] };\n"
                        + "const stateTarget = { kind: 'state', id: 'orders', name: 'orders', state_kind: 'MAP', semantic_parts: { state_key: { kind: 'SCALAR', logical_type: 'BIGINT' }, map_key: { kind: 'SCALAR', logical_type: 'VARCHAR(2147483647)' }, map_value: { kind: 'SCALAR', logical_type: 'BIGINT' } }, serializer_classes: { namespace: 'org.apache.flink.runtime.state.VoidNamespaceSerializer' } };\n"
                        + "const timerTarget = { kind: 'timer', id: '_timer_state/event_t', name: '_timer_state/event_t', state_kind: 'TIMER', semantic_parts: { state_key: { kind: 'SCALAR', logical_type: 'BIGINT' } }, serializer_classes: { namespace: 'org.apache.flink.runtime.state.VoidNamespaceSerializer' } };\n"
                        + "const sinkSql = sinkSourceSql(sinkTarget, meta);\n"
                        + "const sinkItem = sinkOverviewItem(sinkTarget, meta);\n"
                        + "const stateItem = stateOverviewItem(stateTarget, meta);\n"
                        + "const timerItem = stateOverviewItem(timerTarget, meta);\n"
                        + "console.log(JSON.stringify({ sinkSql, sinkItem, stateItem, timerItem }));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        assertEquals(0, exit, stderr);

        assertTrue(stdout.contains("'connector' = 'cobble'"));
        assertTrue(stdout.contains("PRIMARY KEY (`id`) NOT ENFORCED"));
        // Sink DDL column order must follow row_index (name before id) so the connector accepts it.
        assertTrue(stdout.contains("`name` VARCHAR(2147483647)"));
        assertTrue(stdout.indexOf("`name` VARCHAR(2147483647)") < stdout.indexOf("`id` BIGINT"));
        assertTrue(stdout.contains("same source table for scan queries and temporal lookup joins"));
        assertTrue(stdout.contains("'source.kind' = 'state'"));
        assertTrue(stdout.contains("'path' = 'file:///tmp/checkpoints/job-1'"));
        assertTrue(stdout.contains("'state.operator-id' = 'op-1'"));
        assertTrue(stdout.contains("'state.name' = 'orders'"));
        assertTrue(stdout.contains("'state.kind' = 'map'"));
        assertTrue(stdout.contains("PRIMARY KEY (`key`, `map_key`) NOT ENFORCED"));
        assertTrue(stdout.contains("exact-key temporal lookup joins"));
        assertTrue(stdout.contains("\"name\":\"key\""));
        assertTrue(stdout.contains("\"name\":\"map_key\""));
        assertTrue(stdout.contains("\"name\":\"map_value\""));
        assertTrue(stdout.contains("Timer state is visible in the monitor"));
        assertTrue(stdout.contains("\"timerItem\""));
        assertTrue(stdout.contains("Flink SQL source unavailable"));
    }

    private static String readAppJs() throws IOException {
        java.nio.file.Path workspacePath =
                Paths.get("cobble-flink-monitor/src/main/resources/web/app.js");
        if (Files.exists(workspacePath)) {
            return new String(Files.readAllBytes(workspacePath), StandardCharsets.UTF_8);
        }
        try (InputStream input = MonitorAppJsTest.class.getResourceAsStream("/web/app.js")) {
            if (input == null) {
                throw new IOException("web/app.js resource not found");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
