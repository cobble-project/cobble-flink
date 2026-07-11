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

    @Test
    void structuredSemanticTypesRenderFieldTablesAndOverviewLabels() throws Exception {
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
                        + "\nconst rowType = { kind: 'ROW', fields: [\n"
                        + "  { name: 'id', type: { kind: 'SCALAR', logical_type: 'INT' } },\n"
                        + "  { name: 'profile', type: { kind: 'ROW', fields: [{ name: 'region', type: { kind: 'SCALAR', logical_type: 'VARCHAR' } }] } }\n"
                        + "] };\n"
                        + "const tupleType = { kind: 'TUPLE', fields: [\n"
                        + "  { name: 'f0', type: { kind: 'SCALAR', logical_type: 'BIGINT' } },\n"
                        + "  { name: 'f1', type: { kind: 'SCALAR', logical_type: 'VARCHAR' } }\n"
                        + "] };\n"
                        + "const mapType = { kind: 'MAP', key_type: { kind: 'SCALAR', logical_type: 'VARCHAR' }, value_type: rowType };\n"
                        + "const rowFields = semanticTableFields(rowType);\n"
                        + "const tupleFields = semanticTableFields(tupleType);\n"
                        + "const mapFields = semanticTableFields(mapType);\n"
                        + "const scalarInt = { kind: 'SCALAR', logical_type: 'INT' };\n"
                        + "const mapTarget = { kind: 'state', stateKind: 'map', semantic_parts: { state_key: scalarInt, map_key: scalarInt, map_value: rowType } };\n"
                        + "const mapGroups = semanticTableGroups(mapTarget);\n"
                        + "const mapValueGroup = mapGroups.find((group) => group.id === 'map_value');\n"
                        + "const renderedMapCell = renderStateExpandedCells(mapGroups, { state_key: { kind: 'SCALAR', value: 1 }, map_key: { kind: 'SCALAR', value: 2 }, map_value: { kind: 'ROW', fields: [{ name: 'id', value: { kind: 'SCALAR', value: 7 } }, { name: 'profile', value: { kind: 'ROW', fields: [{ name: 'region', value: { kind: 'SCALAR', value: 'west' } }] } }] } }, {}, mapTarget, 'map-row');\n"
                        + "const mapLabel = overviewTypeLabel(mapType);\n"
                        + "const mapHeaderLabel = renderTypeLabel(mapLabel);\n"
                        + "const mapUsable = stateSourceTypeUsable(mapType);\n"
                        + "const renderedMap = renderSemanticTableValue({ kind: 'MAP', entries: [{ key: { kind: 'SCALAR', value: 'a' }, value: { kind: 'ROW', fields: [{ name: 'id', value: { kind: 'SCALAR', value: 7 } }] } }] }, 'map-test');\n"
                        + "const unsupportedFallback = renderDecodeError('value: Unsupported: KryoSerializerSnapshot');\n"
                        + "const restoreFallback = renderDecodeError('value: Failed to restore serializer: ClassNotFoundException');\n"
                        + "console.log(JSON.stringify({ rowFields, tupleFields, mapFields, mapValueGroup, renderedMapCell, mapLabel, mapHeaderLabel, mapUsable, renderedMap, unsupportedFallback, restoreFallback }));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        assertEquals(0, exit, stderr);

        assertTrue(stdout.contains("\"name\":\"id\""));
        assertTrue(stdout.contains("\"logical_type\":\"INT\""));
        assertTrue(stdout.contains("ROW<region VARCHAR>"));
        assertTrue(stdout.contains("\"name\":\"f0\""));
        assertTrue(stdout.contains("\"name\":\"f1\""));
        assertTrue(stdout.contains("MAP<VARCHAR, ROW<"));
        assertTrue(stdout.contains("&lt;<wbr>VARCHAR,<wbr>"));
        assertTrue(stdout.contains("\"id\":\"map_value\""));
        assertTrue(stdout.contains("west"));
        assertFalse(stdout.contains("raw key above"));
        assertTrue(stdout.contains("\"mapUsable\":false"));
        assertTrue(stdout.contains("decoded-map"));
        assertTrue(stdout.contains("key"));
        assertTrue(stdout.contains("value"));
        assertTrue(stdout.contains("Decode fallback"));
        assertTrue(stdout.contains("Raw key and value remain available"));
        assertTrue(stdout.contains("Check that --user-jar includes the serializer"));
    }

    @Test
    void decodeIssuesRenderTypedGuidanceInScanAndTrackChains() throws Exception {
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
                        + "\nconst issueTarget = { kind: 'state', state_kind: 'VALUE', semantic_parts: {\n"
                        + "  state_key: { kind: 'SCALAR', logical_type: 'INT' },\n"
                        + "  value: { kind: 'SCALAR', logical_type: 'VARCHAR' },\n"
                        + "} };\n"
                        + "const fieldLayout = { groups: semanticTableGroups(issueTarget) };\n"
                        // Scan chain: a partial decode retains the raw value fallback cell.
                        + "\nconst scanItem = {\n"
                        + "  bucket: 0, key_b64: 'a2V5', value: { b64: 'dmFsdWU=' },\n"
                        + "  decoded_parts: { state_key: { kind: 'SCALAR', value: 7 } },\n"
                        + "  decode_error: 'value: Failed to restore serializer: ClassNotFoundException',\n"
                        + "  decode_issues: [{ part: 'value', kind: 'SERIALIZER_RESTORE_FAILED', message: 'Failed to restore' }],\n"
                        + "};\n"
                        + "const scanRendered = renderStateScanRow(scanItem, issueTarget, {}, 'scan-issue', fieldLayout);\n"
                        // Track chain: the same partial decode uses camelCase API fields.
                        + "const trackedItem = {\n"
                        + "  id: 'track-issue', targetLabel: 'state', bucket: 0, keyB64: 'a2V5', value: { b64: 'dmFsdWU=' },\n"
                        + "  decodedParts: { state_key: { kind: 'SCALAR', value: 7 } },\n"
                        + "  decodeError: 'value: Failed to restore serializer: ClassNotFoundException',\n"
                        + "  decodeIssues: [{ part: 'value', kind: 'SERIALIZER_RESTORE_FAILED', message: 'Failed to restore' }],\n"
                        + "};\n"
                        + "const trackRendered = renderStateLookupRow(trackedItem, issueTarget, fieldLayout);\n"
                        // CLASSLESS_UNSUPPORTED guidance
                        + "const unsupportedIssues = [{ part: 'value', kind: 'CLASSLESS_UNSUPPORTED', message: 'Non-registered subclass' }];\n"
                        + "const unsupportedRendered = renderDecodeError('value: Non-registered subclass', unsupportedIssues);\n"
                        // MALFORMED_BYTES guidance
                        + "const malformedIssues = [{ part: 'value', kind: 'MALFORMED_BYTES', message: 'Truncated' }];\n"
                        + "const malformedRendered = renderDecodeError('value: Truncated', malformedIssues);\n"
                        // Regex fallback when decode_issues absent
                        + "const fallbackRendered = renderDecodeError('value: Unsupported: KryoSerializerSnapshot');\n"
                        // Empty issues + null error -> empty
                        + "const emptyRendered = renderDecodeError(null, []);\n"
                        + "const decodeFallbackCount = (rendered) => (rendered.match(/Decode fallback/g) || []).length;\n"
                        + "console.log(JSON.stringify({\n"
                        + "  scanRendered, trackRendered,\n"
                        + "  scanFallbackCount: decodeFallbackCount(scanRendered),\n"
                        + "  trackFallbackCount: decodeFallbackCount(trackRendered),\n"
                        + "  scanHasRawValue: scanRendered.includes('dmFsdWU='),\n"
                        + "  trackHasRawValue: trackRendered.includes('dmFsdWU='),\n"
                        + "  scanHasRestoreGuidance: scanRendered.includes('Check that --user-jar includes the serializer class and its dependencies'),\n"
                        + "  trackHasRestoreGuidance: trackRendered.includes('Check that --user-jar includes the serializer class and its dependencies'),\n"
                        + "  unsupportedRendered, malformedRendered, fallbackRendered, emptyRendered\n"
                        + "}));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        assertEquals(0, exit, stderr);

        assertTrue(stdout.contains("\"scanFallbackCount\":1"));
        assertTrue(stdout.contains("\"trackFallbackCount\":1"));
        assertTrue(stdout.contains("\"scanHasRawValue\":true"));
        assertTrue(stdout.contains("\"trackHasRawValue\":true"));
        assertTrue(stdout.contains("\"scanHasRestoreGuidance\":true"));
        assertTrue(stdout.contains("\"trackHasRestoreGuidance\":true"));

        // CLASSLESS_UNSUPPORTED guidance
        assertTrue(
                stdout.contains("A trusted --user-jar may enable this row"),
                "should render CLASSLESS_UNSUPPORTED guidance");

        // MALFORMED_BYTES guidance
        assertTrue(
                stdout.contains("truncated or malformed"),
                "should render MALFORMED_BYTES guidance");

        // Regex fallback when decode_issues absent
        assertTrue(
                stdout.contains("Raw key and value remain available"),
                "regex fallback should still work when decode_issues absent");

        // Empty render when no error and no issues
        assertTrue(
                stdout.contains("\"emptyRendered\":\"\""), "empty render should be empty string");
    }

    @Test
    void timerTimestampUsesDecodedPartsInScanAndTrackFieldTables() throws Exception {
        String appJs = readAppJs();
        String harness =
                "const genericElement = () => ({\n"
                        + "  addEventListener() {}, classList: { toggle() {}, remove() {}, add() {} },\n"
                        + "  setAttribute() {}, querySelectorAll: () => [],\n"
                        + "  closest: () => ({ classList: { toggle() {} } }),\n"
                        + "});\n"
                        + "const resultRows = [];\n"
                        + "const resultBody = genericElement();\n"
                        + "resultBody.appendChild = (row) => resultRows.push(row);\n"
                        + "const resultHead = genericElement();\n"
                        + "const elements = { 'result-body': resultBody, 'result-head': resultHead };\n"
                        + "const document = {\n"
                        + "  getElementById: (id) => elements[id] || (elements[id] = genericElement()),\n"
                        + "  createElement: () => genericElement(),\n"
                        + "  querySelectorAll: () => [],\n"
                        + "  querySelector: () => genericElement(),\n"
                        + "  addEventListener() {},\n"
                        + "};\n"
                        + "const window = { addEventListener() {} };\n"
                        + "async function fetch() { return { ok: true, json: async () => ({}) }; }\n"
                        + appJs
                        + "\nconst timestamp = 1700000000123;\n"
                        + "const timerTarget = {\n"
                        + "  id: 'timer-target', kind: 'timer', name: 'event-time', state_kind: 'TIMER',\n"
                        + "  semantic_parts: { state_key: { kind: 'SCALAR', logical_type: 'BIGINT' } },\n"
                        + "};\n"
                        + "const decodeIssues = [{ part: 'state_key', kind: 'SERIALIZER_RESTORE_FAILED', message: 'Failed to restore' }];\n"
                        + "const scanItem = {\n"
                        + "  bucket: 0, key_b64: 'a2V5', decoded_parts: { timestamp },\n"
                        + "  decode_error: 'state_key: Failed to restore serializer', decode_issues: decodeIssues,\n"
                        + "};\n"
                        + "const trackedItem = {\n"
                        + "  id: 'tracked-timer', targetLabel: 'event-time (timer)', targetKind: 'timer', stateKind: 'TIMER',\n"
                        + "  bucket: 0, keyB64: 'a2V5', semanticParts: timerTarget.semantic_parts,\n"
                        + "  decodedParts: { timestamp }, decodeError: 'state_key: Failed to restore serializer', decodeIssues,\n"
                        + "};\n"
                        + "const summarize = (row) => ({\n"
                        + "  timestampVisible: row.includes(String(timestamp)),\n"
                        + "  fallbackCount: (row.match(/Decode fallback/g) || []).length,\n"
                        + "  guidanceCount: (row.match(/Check that --user-jar includes the serializer class and its dependencies/g) || []).length,\n"
                        + "});\n"
                        + "const renderRows = (fieldTableEnabled) => {\n"
                        + "  state.stateFieldTableEnabled = fieldTableEnabled;\n"
                        + "  resultRows.length = 0;\n"
                        + "  renderScanResult({ scan: { items: [scanItem] }, inspect_target: timerTarget }, { targetId: timerTarget.id, columns: '' });\n"
                        + "  const scan = resultRows[0].innerHTML;\n"
                        + "  resultRows.length = 0;\n"
                        + "  state.trackedLookups = [trackedItem];\n"
                        + "  renderLookupResult();\n"
                        + "  return { scan: summarize(scan), track: summarize(resultRows[0].innerHTML) };\n"
                        + "};\n"
                        + "const fieldTableRows = renderRows(true);\n"
                        + "const rawRows = renderRows(false);\n"
                        + "const legacyTimestamp = renderTimerTimestamp(null, { timestamp: 1700000000456 });\n"
                        + "console.log(JSON.stringify({\n"
                        + "  fieldTableRows, rawRows,\n"
                        + "  legacyTimestampVisible: legacyTimestamp.includes('1700000000456')\n"
                        + "}));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        assertEquals(0, exit, stderr);

        String expectedSummary =
                "{\"timestampVisible\":true,\"fallbackCount\":1,\"guidanceCount\":1}";
        assertTrue(
                stdout.contains(
                        "\"fieldTableRows\":{\"scan\":"
                                + expectedSummary
                                + ",\"track\":"
                                + expectedSummary
                                + "}"));
        assertTrue(
                stdout.contains(
                        "\"rawRows\":{\"scan\":"
                                + expectedSummary
                                + ",\"track\":"
                                + expectedSummary
                                + "}"));
        assertTrue(stdout.contains("\"legacyTimestampVisible\":true"));
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
