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
    void usesOnlySessionApiAndSwapsLatestAfterReplacementOverviewSucceeds() throws Exception {
        String appJs = readAppJs();

        assertFalse(appJs.contains("/api/v1/meta"));
        assertFalse(appJs.contains("/api/v1/mode"));
        assertFalse(appJs.contains("/api/v1/inspect"));
        assertTrue(appJs.contains("/api/v1/discovery"));
        assertTrue(appJs.contains("/api/v1/sessions"));
        int overview = appJs.indexOf("replacementOverview = await request");
        int swap = appJs.indexOf("state.sessionId = replacement.session_id");
        int deleteOld = appJs.indexOf("/api/v1/sessions/${previousId}");
        assertTrue(overview >= 0 && overview < swap);
        assertTrue(swap < deleteOld);
        assertTrue(appJs.contains("window.COBBLE_MONITOR_INITIAL_SOURCE"));
        assertTrue(appJs.contains("result.found"));
        assertTrue(appJs.contains("BigInt(value)"));
        assertTrue(appJs.contains("typedValue = integer.toString()"));
    }

    @Test
    void latestSessionFailuresRecoverOnceAndPreserveRequestState() throws Exception {
        String appJs = readAppJs();
        String harness =
                "const elements = {};\n"
                        + "const element = () => ({\n"
                        + "  value: '', checked: false, disabled: false, textContent: '', innerHTML: '', dataset: {},\n"
                        + "  classList: { toggle() {}, add() {}, remove() {}, contains() { return false } },\n"
                        + "  addEventListener() {}, appendChild() {}, querySelectorAll() { return [] },\n"
                        + "  querySelector() { return element() }, setAttribute() {}, closest() { return null },\n"
                        + "});\n"
                        + "const document = {\n"
                        + "  getElementById: (id) => elements[id] || (elements[id] = element()),\n"
                        + "  createElement: () => element(), querySelectorAll: () => [], querySelector: () => element(),\n"
                        + "  addEventListener() {},\n"
                        + "};\n"
                        + "const window = { addEventListener() {} };\n"
                        + "let fetch = async () => { throw new Error('unexpected fetch') };\n"
                        + appJs
                        + "\nconst target = { id: 'raw', name: 'raw', kind: 'state', allows_columns: false };\n"
                        + "const catalog = { checkpoints: [{ checkpoint_id: 11, directory: '/chk-11', operators: [{ id: 'op' }] }] };\n"
                        + "const replacement = { session_id: 'new', source: 'file:///root', source_kind: 'checkpoint', checkpoint_id: 11, operator_id: 'op', targets: [target], total_buckets: 1 };\n"
                        + "const success = (body) => ({ ok: true, status: 200, json: async () => body });\n"
                        + "const gone = (code) => ({ ok: false, status: 410, json: async () => ({ code, message: code }) });\n"
                        + "const setup = (selectedLatest = true) => {\n"
                        + "  state.sessionId = 'old'; state.sourcePath = 'file:///root'; state.selectedLatest = selectedLatest;\n"
                        + "  state.sessionRecoveryPromise = null; state.inspectMode = 'scan'; state.trackedLookups = [];\n"
                        + "  state.meta = { source_open: true, source: 'file:///root', source_kind: 'checkpoint', selected_checkpoint_id: 10, selected_operator_id: 'op', inspect_targets: [target], total_buckets: 1, overview: {} };\n"
                        + "  state.previousPageCursors = []; state.currentPageCursor = null; state.nextPageCursor = null; state.pageNumber = 1; state.scanHasResult = false;\n"
                        + "  elements['inspect-target'].value = 'raw'; elements['limit'].value = '50'; elements['bucket'].value = 'all'; elements['prefix'].value = 'keep-filter';\n"
                        + "};\n"
                        + "const installFetch = (failureCode, calls, retryFailureCode = null) => {\n"
                        + "  fetch = async (path, options = {}) => {\n"
                        + "    const body = options.body ? JSON.parse(options.body) : null; calls.push({ path, method: options.method || 'GET', body });\n"
                        + "    if (path === '/api/v1/discovery') return success(catalog);\n"
                        + "    if (path === '/api/v1/sessions' && options.method === 'POST') return success(replacement);\n"
                        + "    if (path === '/api/v1/sessions/new/overview') return success({ items: [] });\n"
                        + "    if (path.includes('/scan')) return path.includes('/old/') ? gone(failureCode) : retryFailureCode ? gone(retryFailureCode) : success({ rows: [] });\n"
                        + "    if (path.includes('/lookup')) return path.includes('/old/') ? gone(failureCode) : retryFailureCode ? gone(retryFailureCode) : success({ rows: [{ key_b64: 'aw==', value_b64: 'dg==', found: true }] });\n"
                        + "    if (path.includes('/concurrent-')) return path.includes('/old/') ? gone(failureCode) : success({});\n"
                        + "    return success({});\n"
                        + "  };\n"
                        + "};\n"
                        + "setup(true);\n"
                        + "state.previousPageCursors = [null]; state.currentPageCursor = 'old-page'; state.nextPageCursor = 'next-page'; state.pageNumber = 2; state.scanHasResult = true;\n"
                        + "const scanCalls = []; installFetch('SESSION_EXPIRED', scanCalls); await runScan('refresh');\n"
                        + "const scanRequests = scanCalls.filter((call) => call.path.includes('/scan'));\n"
                        + "const latest410 = scanRequests.length === 2 && scanRequests[0].body.page_token === 'old-page' && !('page_token' in scanRequests[1].body) && state.pageNumber === 1 && elements['prefix'].value === 'keep-filter';\n"
                        + "const singleRetry = scanCalls.filter((call) => call.path === '/api/v1/sessions' && call.method === 'POST').length === 1;\n"
                        + "setup(true); state.inspectMode = 'lookup'; const tracked = { id: 'tracked', targetId: 'raw', columns: '', bucket: 0, keyB64: 'aw==', keyUtf8: 'k' }; state.trackedLookups = [tracked];\n"
                        + "const lookupCalls = []; installFetch('CHECKPOINT_UNAVAILABLE', lookupCalls); await runLookup();\n"
                        + "const lookupPreserved = state.trackedLookups.length === 1 && state.trackedLookups[0] === tracked && state.trackedLookups[0].found === true;\n"
                        + "const latestMissingFile = lookupCalls.filter((call) => call.path.includes('/lookup')).length === 2 && lookupCalls.filter((call) => call.path === '/api/v1/sessions' && call.method === 'POST').length === 1;\n"
                        + "const fixedCheckpointMessage = 'The selected checkpoint is incomplete or expired. Choose Latest or open another checkpoint.';\n"
                        + "const fixedCheckpoint = async (code) => { setup(false); const calls = []; installFetch(code, calls); let message = ''; try { await withSessionRecovery(() => request(`/api/v1/sessions/${state.sessionId}/scan`)); } catch (error) { message = error.message; } return message === fixedCheckpointMessage && calls.filter((call) => call.path === '/api/v1/discovery').length === 0; };\n"
                        + "const fixedCheckpointUnavailable = await fixedCheckpoint('CHECKPOINT_UNAVAILABLE');\n"
                        + "const fixedSessionExpired = await fixedCheckpoint('SESSION_EXPIRED');\n"
                        + "const retryStopsAfterRecovery = async (code) => { setup(true); const calls = []; installFetch(code, calls, code); let retryCode = ''; try { await withSessionRecovery(() => request(`/api/v1/sessions/${state.sessionId}/scan`)); } catch (error) { retryCode = error.code; } const scans = calls.filter((call) => call.path.includes('/scan')); const replacements = calls.filter((call) => call.path === '/api/v1/sessions' && call.method === 'POST'); return retryCode === code && scans.length === 2 && replacements.length === 1; };\n"
                        + "const retrySessionExpiredStops = await retryStopsAfterRecovery('SESSION_EXPIRED');\n"
                        + "const retryCheckpointUnavailableStops = await retryStopsAfterRecovery('CHECKPOINT_UNAVAILABLE');\n"
                        + "setup(true); const concurrentCalls = []; installFetch('SESSION_EXPIRED', concurrentCalls); await Promise.all([withSessionRecovery(() => request(`/api/v1/sessions/${state.sessionId}/concurrent-a`)), withSessionRecovery(() => request(`/api/v1/sessions/${state.sessionId}/concurrent-b`))]);\n"
                        + "const concurrentDedup = concurrentCalls.filter((call) => call.path === '/api/v1/sessions' && call.method === 'POST').length === 1;\n"
                        + "console.log(JSON.stringify({ latest410, latestMissingFile, fixedCheckpointUnavailable, fixedSessionExpired, singleRetry, retrySessionExpiredStops, retryCheckpointUnavailableStops, concurrentDedup, lookupPreserved }));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();

        assertEquals(0, exit, stderr);
        assertTrue(stdout.contains("\"latest410\":true"));
        assertTrue(stdout.contains("\"latestMissingFile\":true"));
        assertTrue(stdout.contains("\"fixedCheckpointUnavailable\":true"));
        assertTrue(stdout.contains("\"fixedSessionExpired\":true"));
        assertTrue(stdout.contains("\"singleRetry\":true"));
        assertTrue(stdout.contains("\"retrySessionExpiredStops\":true"));
        assertTrue(stdout.contains("\"retryCheckpointUnavailableStops\":true"));
        assertTrue(stdout.contains("\"concurrentDedup\":true"));
        assertTrue(stdout.contains("\"lookupPreserved\":true"));
    }

    @Test
    void latestRecoveryFollowsOnlyNewCheckpointIds() throws Exception {
        String appJs = readAppJs();
        String harness =
                "const elements = {};\n"
                        + "const element = () => ({\n"
                        + "  value: '', checked: false, disabled: false, textContent: '', innerHTML: '', dataset: {},\n"
                        + "  classList: { toggle() {}, add() {}, remove() {}, contains() { return false } },\n"
                        + "  addEventListener() {}, appendChild() {}, querySelectorAll() { return [] },\n"
                        + "  querySelector() { return element() }, setAttribute() {}, closest() { return null },\n"
                        + "});\n"
                        + "const document = {\n"
                        + "  getElementById: (id) => elements[id] || (elements[id] = element()),\n"
                        + "  createElement: () => element(), querySelectorAll: () => [], querySelector: () => element(),\n"
                        + "  addEventListener() {},\n"
                        + "};\n"
                        + "const window = { addEventListener() {} };\n"
                        + "let fetch = async () => { throw new Error('unexpected fetch') };\n"
                        + appJs
                        + "\nconst target = { id: 'raw', name: 'raw', kind: 'state', allows_columns: false };\n"
                        + "const success = (body) => ({ ok: true, status: 200, json: async () => body });\n"
                        + "const gone = () => ({ ok: false, status: 410, json: async () => ({ code: 'SESSION_EXPIRED', message: 'expired' }) });\n"
                        + "const setup = (checkpointId = 10) => {\n"
                        + "  state.sessionId = `session-${checkpointId}`; state.sourcePath = 'file:///root'; state.selectedLatest = true; state.sessionRecoveryPromise = null; state.sessionRecoveryCheckpointId = null;\n"
                        + "  state.meta = { source_open: true, source: 'file:///root', source_kind: 'checkpoint', selected_checkpoint_id: checkpointId, selected_operator_id: 'op', inspect_targets: [target], total_buckets: 1, overview: {} };\n"
                        + "  state.trackedLookups = []; state.previousPageCursors = []; state.currentPageCursor = null; state.nextPageCursor = null; state.pageNumber = 1; state.scanHasResult = false;\n"
                        + "  elements['inspect-target'].value = 'raw'; elements['limit'].value = '50'; elements['bucket'].value = 'all'; elements['prefix'].value = '';\n"
                        + "};\n"
                        + "const installTransitions = (latestIds, failingSessionIds, calls) => {\n"
                        + "  let discoveryIndex = 0; let latestId = null;\n"
                        + "  fetch = async (path, options = {}) => {\n"
                        + "    calls.push({ path, method: options.method || 'GET' });\n"
                        + "    if (path === '/api/v1/discovery') { latestId = latestIds[Math.min(discoveryIndex, latestIds.length - 1)]; discoveryIndex += 1; return success({ checkpoints: [{ checkpoint_id: latestId, directory: `/chk-${latestId}`, operators: [{ id: 'op' }] }] }); }\n"
                        + "    if (path === '/api/v1/sessions' && options.method === 'POST') return success({ session_id: `session-${latestId}`, source: 'file:///root', source_kind: 'checkpoint', checkpoint_id: latestId, operator_id: 'op', targets: [target], total_buckets: 1 });\n"
                        + "    if (path.includes('/overview')) return success({ items: [] });\n"
                        + "    if (path.includes('/operation/')) { const sessionId = path.split('/').pop(); return failingSessionIds.has(sessionId) ? gone() : success({}); }\n"
                        + "    return success({});\n"
                        + "  };\n"
                        + "};\n"
                        + "const operation = () => withSessionRecovery(() => request(`/operation/${state.sessionId}`));\n"
                        + "const attempts = (calls) => calls.filter((call) => call.path.includes('/operation/')).map((call) => call.path.split('/').pop());\n"
                        + "const replacements = (calls) => calls.filter((call) => call.path === '/api/v1/sessions' && call.method === 'POST').length;\n"
                        + "setup(); const sameCalls = []; installTransitions([10], new Set(['session-10']), sameCalls); let sameError = null; try { await operation(); } catch (error) { sameError = error; }\n"
                        + "const sameIdStops = sameError?.code === 'SESSION_EXPIRED' && attempts(sameCalls).length === 1 && replacements(sameCalls) === 0;\n"
                        + "setup(); const lowerCalls = []; installTransitions([9], new Set(['session-10']), lowerCalls); let lowerError = null; try { await operation(); } catch (error) { lowerError = error; }\n"
                        + "const lowerIdStops = lowerError?.code === 'SESSION_EXPIRED' && attempts(lowerCalls).length === 1 && replacements(lowerCalls) === 0;\n"
                        + "setup(); const sequenceCalls = []; installTransitions([11, 12], new Set(['session-10', 'session-11']), sequenceCalls); await operation(); const sequenceAttempts = attempts(sequenceCalls);\n"
                        + "const newerSequenceSucceeds = sequenceAttempts.join(',') === 'session-10,session-11,session-12' && replacements(sequenceCalls) === 2;\n"
                        + "const noCheckpointTwice = new Set(sequenceAttempts).size === sequenceAttempts.length;\n"
                        + "setup(); const cappedCalls = []; installTransitions([11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21], new Set(['session-10', 'session-11', 'session-12', 'session-13', 'session-14', 'session-15', 'session-16', 'session-17', 'session-18', 'session-19', 'session-20']), cappedCalls); let cappedError = null; try { await operation(); } catch (error) { cappedError = error; }\n"
                        + "const transitionCapStops = cappedError?.code === 'SESSION_EXPIRED' && attempts(cappedCalls).length === 11 && replacements(cappedCalls) === 10;\n"
                        + "setup(); const concurrentCalls = []; installTransitions([11, 12], new Set(['session-10', 'session-11']), concurrentCalls); await Promise.all([operation(), operation()]);\n"
                        + "const concurrentTransitionsShared = replacements(concurrentCalls) === 2 && concurrentCalls.filter((call) => call.path === '/api/v1/discovery').length === 2;\n"
                        + "setup(11); const overlapCalls = []; installTransitions([12], new Set(['session-11']), overlapCalls); state.sessionRecoveryPromise = new Promise(() => {}); state.sessionRecoveryCheckpointId = 10; await Promise.all([operation(), operation()]);\n"
                        + "const crossGenerationDedup = replacements(overlapCalls) === 1 && overlapCalls.filter((call) => call.path === '/api/v1/discovery').length === 1 && state.sessionId === 'session-12';\n"
                        + "console.log(JSON.stringify({ sameIdStops, lowerIdStops, newerSequenceSucceeds, transitionCapStops, noCheckpointTwice, concurrentTransitionsShared, crossGenerationDedup }));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();

        assertEquals(0, exit, stderr);
        assertTrue(stdout.contains("\"sameIdStops\":true"));
        assertTrue(stdout.contains("\"lowerIdStops\":true"));
        assertTrue(stdout.contains("\"newerSequenceSucceeds\":true"));
        assertTrue(stdout.contains("\"transitionCapStops\":true"));
        assertTrue(stdout.contains("\"noCheckpointTwice\":true"));
        assertTrue(stdout.contains("\"concurrentTransitionsShared\":true"));
        assertTrue(stdout.contains("\"crossGenerationDedup\":true"));
    }

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
    void overviewRendersSdkProvidedSqlWithoutClientDerivation() throws Exception {
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
                        + "\nconst meta = { overview: { items: [{ id: 'orders', title: 'orders', kind: 'VALUE', detail: 'keyed state', fields: [{ role: 'State key', name: 'id', logical_type: 'BIGINT' }], source_sql: { ddl: 'CREATE TABLE `orders` (`id` BIGINT)', note: 'Pinned source' } }] } };\n"
                        + "const item = overviewItems(meta)[0];\n"
                        + "console.log(JSON.stringify({ item, html: renderOverviewItem(item) }));\n";

        Process process = new ProcessBuilder("node", "--input-type=module", "-").start();
        process.getOutputStream().write(harness.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().close();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();

        assertEquals(0, exit, stderr);
        assertTrue(stdout.contains("CREATE TABLE `orders` (`id` BIGINT)"));
        assertTrue(stdout.contains("Pinned source"));
        assertTrue(stdout.contains("State key"));
        assertFalse(appJs.contains("function sinkSourceSql"));
        assertFalse(appJs.contains("function stateSourceSql"));
        assertFalse(appJs.contains("selectedCheckpointForSql"));
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
                        + "const mapLabel = semanticDisplayTypeLabel(mapType);\n"
                        + "const mapHeaderLabel = renderTypeLabel(mapLabel);\n"
                        + "const renderedMap = renderSemanticTableValue({ kind: 'MAP', entries: [{ key: { kind: 'SCALAR', value: 'a' }, value: { kind: 'ROW', fields: [{ name: 'id', value: { kind: 'SCALAR', value: 7 } }] } }] }, 'map-test');\n"
                        + "const unsupportedFallback = renderDecodeError('value: Unsupported: KryoSerializerSnapshot');\n"
                        + "const restoreFallback = renderDecodeError('value: Failed to restore serializer: ClassNotFoundException');\n"
                        + "console.log(JSON.stringify({ rowFields, tupleFields, mapFields, mapValueGroup, renderedMapCell, mapLabel, mapHeaderLabel, renderedMap, unsupportedFallback, restoreFallback }));\n";

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
