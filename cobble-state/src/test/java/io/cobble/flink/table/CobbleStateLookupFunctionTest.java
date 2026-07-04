package io.cobble.flink.table;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.ScanCursor;
import io.cobble.ScanOptions;
import io.cobble.flink.common.inspect.InspectSchemaRegistryLayout;
import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Direct lookup function tests against a real MiniCluster-produced Cobble state checkpoint. The
 * fixture mirrors {@link CobbleStateSourceProofITTest}: keyed value-like state jobs run until a
 * checkpoint completes, then {@link CobbleStateLookupFunction} is opened and exercised for hit,
 * miss, null-key, workspace cleanup, streaming rejection, reducing state, and aggregating state.
 */
class CobbleStateLookupFunctionTest {

    private static final int PARALLELISM = 4;

    @TempDir private Path tempDir;

    @Test
    void valueLookupReturnsOneRowForHit() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        // Scan first to discover a real key + its accumulated value.
        StateSourceConfig scanConfig =
                stateSourceConfig(
                        checkpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        discovered.f1,
                        "latest",
                        "batch");
        List<RowData> scanRows = drainStateRows(scanConfig);
        assertFalse(scanRows.isEmpty(), "expected at least one value-state row from scan");
        RowData firstRow = scanRows.get(0);
        int knownKey = firstRow.getInt(0);
        int knownValue = firstRow.getInt(1);

        // Build a lookup config with a present contract (PK = key).
        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        "latest",
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0});
        try {
            lookup.open(new FunctionContext(null));
            Collection<RowData> result = lookup.lookup(singleIntRow(knownKey));
            assertEquals(1, result.size(), "expected exactly one row for a hit");
            RowData hit = result.iterator().next();
            assertEquals(knownKey, hit.getInt(0), "key column mismatch");
            assertEquals(knownValue, hit.getInt(1), "value column mismatch");
        } finally {
            lookup.close();
        }
    }

    @Test
    void valueLookupReturnsEmptyForMiss() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0});
        try {
            lookup.open(new FunctionContext(null));
            // A very large key that was never produced by the source (keys are value % 4 = 0..3).
            Collection<RowData> result = lookup.lookup(singleIntRow(999_999));
            assertTrue(result.isEmpty(), "expected empty collection for a miss");
        } finally {
            lookup.close();
        }
    }

    @Test
    void valueLookupRejectsNullKeyColumn() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0});
        try {
            lookup.open(new FunctionContext(null));
            GenericRowData nullKeyRow = new GenericRowData(1);
            nullKeyRow.setField(0, null);
            IOException error = assertThrows(IOException.class, () -> lookup.lookup(nullKeyRow));
            assertTrue(error.getMessage().contains("'key'"), error.getMessage());
        } finally {
            lookup.close();
        }
    }

    @Test
    void streamingLatestStateLookupFailsClearly() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig streamingConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        "latest",
                        "streaming");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(streamingConfig, new int[] {0});
        try {
            IOException error =
                    assertThrows(IOException.class, () -> lookup.open(new FunctionContext(null)));
            assertTrue(error.getMessage().contains("scan.mode='batch'"), error.getMessage());
        } finally {
            lookup.close();
        }
    }

    @Test
    void lookupClosesTemporaryReaderWorkspace() throws Exception {
        Tuple2<String, Long> discovered = runStatefulJob();
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "value-state",
                        "value",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0});
        // Snapshot the set of cobble-state-source-* temp dirs before opening, so stale dirs from
        // other tests or parallel runs don't cause false failures.
        Set<String> before = cobbleStateSourceTempDirs();
        lookup.open(new FunctionContext(null));
        lookup.close();
        Set<String> after = cobbleStateSourceTempDirs();
        assertEquals(before, after, "temporary reader workspace not cleaned after close()");
    }

    // ------------------------------------------------------------------------------------------
    //  MapState lookup tests
    // ------------------------------------------------------------------------------------------

    @Test
    void mapLookupReturnsOneRowForHit() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-map");
        Path localState = tempDir.resolve("local-state-map");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, MapStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        // Scan to discover a real (state key, map key, map value) triple.
        StateSourceConfig scanConfig =
                stateSourceConfig(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        discovered.f1,
                        "latest",
                        "batch");
        List<RowData> scanRows = drainStateRows(scanConfig);
        assertFalse(scanRows.isEmpty(), "expected at least one map-state row from scan");
        RowData firstRow = scanRows.get(0);
        int knownKey = firstRow.getInt(0);
        int knownMapKey = firstRow.getInt(1);
        int knownMapValue = firstRow.getInt(2);

        // Lookup config: PK = (key, map_key) — full map-entry key.
        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0, 1});
        try {
            lookup.open(new FunctionContext(null));
            Collection<RowData> result = lookup.lookup(twoIntRow(knownKey, knownMapKey));
            assertEquals(1, result.size(), "expected exactly one row for a map-state hit");
            RowData hit = result.iterator().next();
            assertEquals(knownKey, hit.getInt(0), "key column mismatch");
            assertEquals(knownMapKey, hit.getInt(1), "map_key column mismatch");
            assertEquals(knownMapValue, hit.getInt(2), "map_value column mismatch");
        } finally {
            lookup.close();
        }
    }

    @Test
    void mapLookupReturnsEmptyForMissingMapKey() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-map-miss");
        Path localState = tempDir.resolve("local-state-map-miss");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, MapStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0, 1});
        try {
            lookup.open(new FunctionContext(null));
            // Key 0 exists (value % 4 = 0), but map key 999_999 was never written.
            Collection<RowData> result = lookup.lookup(twoIntRow(0, 999_999));
            assertTrue(result.isEmpty(), "expected empty collection for a missing map key");
        } finally {
            lookup.close();
        }
    }

    @Test
    void mapLookupReturnsEmptyForMissingStateKey() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-map-miss-key");
        Path localState = tempDir.resolve("local-state-map-miss-key");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, MapStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0, 1});
        try {
            lookup.open(new FunctionContext(null));
            // State key 999_999 was never written (keys are 0..3).
            Collection<RowData> result = lookup.lookup(twoIntRow(999_999, 0));
            assertTrue(result.isEmpty(), "expected empty collection for a missing state key");
        } finally {
            lookup.close();
        }
    }

    @Test
    void mapLookupRejectsNullStateKey() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-map-null-key");
        Path localState = tempDir.resolve("local-state-map-null-key");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, MapStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0, 1});
        try {
            lookup.open(new FunctionContext(null));
            GenericRowData nullKeyRow = new GenericRowData(2);
            nullKeyRow.setField(0, null);
            nullKeyRow.setField(1, 0);
            IOException error = assertThrows(IOException.class, () -> lookup.lookup(nullKeyRow));
            assertTrue(error.getMessage().contains("'key'"), error.getMessage());
        } finally {
            lookup.close();
        }
    }

    @Test
    void mapLookupRejectsNullMapKey() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-map-null-mk");
        Path localState = tempDir.resolve("local-state-map-null-mk");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, MapStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0, 1});
        try {
            lookup.open(new FunctionContext(null));
            GenericRowData nullMapKeyRow = new GenericRowData(2);
            nullMapKeyRow.setField(0, 0);
            nullMapKeyRow.setField(1, null);
            IOException error = assertThrows(IOException.class, () -> lookup.lookup(nullMapKeyRow));
            assertTrue(error.getMessage().contains("'map_key'"), error.getMessage());
        } finally {
            lookup.close();
        }
    }

    @Test
    void mapLookupReturnsOneRowWithNullValueForPresentNullEntry() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-map-present-null");
        Path localState = tempDir.resolve("local-state-map-present-null");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        // Use the NullValueMapStateMapper which writes mapState.put(mapKey, null).
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, NullValueMapStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        // Scan to discover a real (state key, map key) pair. The map value should be null.
        StateSourceConfig scanConfig =
                stateSourceConfig(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        discovered.f1,
                        "latest",
                        "batch");
        List<RowData> scanRows = drainStateRows(scanConfig);
        assertFalse(scanRows.isEmpty(), "expected at least one map-state row from scan");
        RowData firstRow = scanRows.get(0);
        int knownKey = firstRow.getInt(0);
        int knownMapKey = firstRow.getInt(1);
        assertTrue(
                firstRow.isNullAt(2), "scan row should have null map_value for present-null entry");

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "map-state",
                        "map",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0),
                                new StateSourceField(
                                        "map_value", "INT", StateSourceField.Group.MAP_VALUE, 0)),
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "map_key", "INT", StateSourceField.Group.MAP_KEY, 0)),
                        new int[] {0, 1},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0, 1});
        try {
            lookup.open(new FunctionContext(null));
            Collection<RowData> result = lookup.lookup(twoIntRow(knownKey, knownMapKey));
            assertEquals(1, result.size(), "expected exactly one row for a present-null map entry");
            RowData hit = result.iterator().next();
            assertEquals(knownKey, hit.getInt(0), "key column mismatch");
            assertEquals(knownMapKey, hit.getInt(1), "map_key column mismatch");
            assertTrue(hit.isNullAt(2), "map_value should be null for present-null entry");
        } finally {
            lookup.close();
        }
    }

    /**
     * Returns the set of {@code cobble-state-source-*} directory names currently in java.io.tmpdir.
     */
    private static Set<String> cobbleStateSourceTempDirs() {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        String[] names = tmpDir.list((dir, name) -> name.startsWith("cobble-state-source-"));
        if (names == null) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(names));
    }

    @Test
    void reducingLookupReturnsReducedValue() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-reducing");
        Path localState = tempDir.resolve("local-state-reducing");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, ReducingStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        // Scan to discover a real key + its reduced value.
        StateSourceConfig scanConfig =
                stateSourceConfig(
                        checkpointRootUri,
                        operatorId,
                        "reducing-state",
                        "reducing",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        discovered.f1,
                        "latest",
                        "batch");
        List<RowData> scanRows = drainStateRows(scanConfig);
        assertFalse(scanRows.isEmpty(), "expected at least one reducing-state row from scan");
        RowData firstRow = scanRows.get(0);
        int knownKey = firstRow.getInt(0);
        int knownValue = firstRow.getInt(1);

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "reducing-state",
                        "reducing",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0});
        try {
            lookup.open(new FunctionContext(null));
            Collection<RowData> result = lookup.lookup(singleIntRow(knownKey));
            assertEquals(1, result.size(), "expected exactly one row for a reducing-state hit");
            RowData hit = result.iterator().next();
            assertEquals(knownKey, hit.getInt(0), "key column mismatch");
            assertEquals(knownValue, hit.getInt(1), "reduced value mismatch");
        } finally {
            lookup.close();
        }
    }

    @Test
    void aggregatingLookupReturnsAccumulatorValue() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints-aggregating");
        Path localState = tempDir.resolve("local-state-aggregating");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        Tuple2<String, Long> discovered =
                runStatefulJob(checkpointRoot, localState, AggregatingStateMapper::new);
        String operatorId = discovered.f0.split("@", 2)[0];
        String checkpointRootUri = discovered.f0.split("@", 2)[1];

        // Scan to discover a real key + its accumulator value.
        StateSourceConfig scanConfig =
                stateSourceConfig(
                        checkpointRootUri,
                        operatorId,
                        "aggregating-state",
                        "aggregating",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        discovered.f1,
                        "latest",
                        "batch");
        List<RowData> scanRows = drainStateRows(scanConfig);
        assertFalse(scanRows.isEmpty(), "expected at least one aggregating-state row from scan");
        RowData firstRow = scanRows.get(0);
        int knownKey = firstRow.getInt(0);
        int knownValue = firstRow.getInt(1);

        StateSourceConfig lookupConfig =
                stateSourceConfigWithContract(
                        checkpointRootUri,
                        operatorId,
                        "aggregating-state",
                        "aggregating",
                        Arrays.asList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0),
                                new StateSourceField(
                                        "value", "INT", StateSourceField.Group.VALUE, 0)),
                        Collections.singletonList(
                                new StateSourceField(
                                        "key", "INT", StateSourceField.Group.STATE_KEY, 0)),
                        new int[] {0},
                        String.valueOf(discovered.f1),
                        "batch");

        CobbleStateLookupFunction lookup =
                new CobbleStateLookupFunction(lookupConfig, new int[] {0});
        try {
            lookup.open(new FunctionContext(null));
            Collection<RowData> result = lookup.lookup(singleIntRow(knownKey));
            assertEquals(1, result.size(), "expected exactly one row for an aggregating-state hit");
            RowData hit = result.iterator().next();
            assertEquals(knownKey, hit.getInt(0), "key column mismatch");
            assertEquals(knownValue, hit.getInt(1), "accumulator value mismatch");
        } finally {
            lookup.close();
        }
    }

    // ------------------------------------------------------------------------------------------
    //  MiniCluster fixture (mirrors CobbleStateSourceProofITTest)
    // ------------------------------------------------------------------------------------------

    private Tuple2<String, Long> runStatefulJob() throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints");
        Path localState = tempDir.resolve("local-state");
        Files.createDirectories(checkpointRoot);
        Files.createDirectories(localState);
        return runStatefulJob(checkpointRoot, localState, ValueStateMapper::new);
    }

    private Tuple2<String, Long> runStatefulJob(
            Path checkpointRoot,
            Path localState,
            Supplier<RichMapFunction<Integer, Integer>> mapperFactory)
            throws Exception {
        org.apache.flink.configuration.Configuration clusterConfiguration =
                new org.apache.flink.configuration.Configuration();
        clusterConfiguration.setString(
                org.apache.flink.configuration.HighAvailabilityOptions.HA_MODE,
                io.cobble.flink.state.CobbleHighAvailabilityServicesFactory.class.getName());
        clusterConfiguration.setString("cobble.ha.delegate.type", "NONE");
        clusterConfiguration.setString(
                org.apache.flink.configuration.JobManagerOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(
                org.apache.flink.configuration.JobManagerOptions.PORT, 6123);
        clusterConfiguration.setString(
                org.apache.flink.configuration.RestOptions.ADDRESS, "localhost");
        clusterConfiguration.setInteger(org.apache.flink.configuration.RestOptions.PORT, 0);
        clusterConfiguration.set(
                org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointRoot.toUri().toString());

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(clusterConfiguration)
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        cluster.before();
        try {
            Configuration jobConfig = new Configuration();
            jobConfig.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointRoot.toUri().toString());
            jobConfig.set(CobbleOptions.LOCAL_DIRECTORIES, localState.toString());
            jobConfig.set(CobbleOptions.MEMTABLE_BUFFER_RATIO, 0.25d);
            jobConfig.set(CobbleOptions.MEMTABLE_BUFFER_COUNT, 4);
            jobConfig.set(CobbleOptions.DIRECT_IO_BUFFER_SIZE, MemorySize.parse("8kb"));
            jobConfig.set(CobbleOptions.DIRECT_IO_BUFFER_POOL_MAX_SIZE, 128);

            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(5_000L);
            env.getCheckpointConfig().setCheckpointTimeout(180_000L);
            env.getCheckpointConfig()
                    .setExternalizedCheckpointCleanup(
                            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setStateBackend(
                    new CobbleStateBackend().configure(jobConfig, getClass().getClassLoader()));

            env.addSource(new ContinuousIntegerSource())
                    .name("lookup-source")
                    .uid("lookup-source")
                    .keyBy(value -> value % PARALLELISM)
                    .map(mapperFactory.get())
                    .name("lookup-stateful")
                    .uid("lookup-stateful")
                    .setMaxParallelism(128);

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            JobID jobId = jobGraph.getJobID();
            cluster.getClusterClient().submitJob(jobGraph).get(30, TimeUnit.SECONDS);
            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
            cluster.getMiniCluster().triggerCheckpoint(jobId).get(30, TimeUnit.SECONDS);
            waitForCompletedCheckpoint(cluster.getMiniCluster(), jobId, 1, Duration.ofSeconds(60));
            Thread.sleep(2_000L);
            try {
                cluster.getClusterClient().cancel(jobId).get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // The job may have already finished; the checkpoint artifacts are already on disk.
            }
        } finally {
            cluster.after();
        }

        return discoverOperatorAndCheckpoint(checkpointRoot);
    }

    private static List<RowData> drainStateRows(StateSourceConfig config) throws Exception {
        List<CobbleStateSourceSplit> splits =
                CobbleStateSourceRuntime.createStateSourceSplits(config);
        assertFalse(splits.isEmpty(), "expected at least one split");
        CobbleStateSourceRuntime.ReaderHandle handle =
                CobbleStateSourceRuntime.openReader(config, splits.get(0).checkpointId);
        CobbleStateSourceRuntime.RuntimeSchema runtimeSchema =
                CobbleStateSourceRuntime.loadRuntimeSchema(config);
        CobbleStateRowDecoder decoder = new CobbleStateRowDecoder(config, runtimeSchema);
        String columnFamily = runtimeSchema.schema.columnFamily();
        List<RowData> rows = new ArrayList<>();
        try {
            for (CobbleStateSourceSplit split : splits) {
                for (int keyGroup = split.keyGroupStart;
                        keyGroup <= split.keyGroupEnd;
                        keyGroup++) {
                    ScanOptions options =
                            CobbleStateSourceRuntime.scanOptions(columnFamily, Integer.MAX_VALUE);
                    ScanCursor cursor;
                    try {
                        cursor =
                                handle.reader.scanWithOptions(
                                        keyGroup,
                                        CobbleStateSourceRuntime.emptyScanKey(),
                                        CobbleStateSourceRuntime.maxScanKey(),
                                        options);
                    } catch (RuntimeException e) {
                        if (e.getMessage() != null
                                && e.getMessage().contains("Unknown column family")) {
                            continue;
                        }
                        throw e;
                    }
                    try (ScanCursor scan = cursor) {
                        ScanCursor.Entry entry = scan.nextEntry();
                        while (entry != null) {
                            rows.addAll(
                                    decoder.decode(
                                            entry.key, entry.columns, split.splitId(), keyGroup));
                            entry = scan.nextEntry();
                        }
                    }
                }
            }
        } finally {
            handle.close();
        }
        return rows;
    }

    private static Tuple2<String, Long> discoverOperatorAndCheckpoint(Path checkpointRoot)
            throws Exception {
        String operatorId = null;
        Path checkpointDir = null;
        long latestCheckpointId = -1L;
        try (Stream<Path> walk = Files.walk(checkpointRoot)) {
            for (Path path : (Iterable<Path>) walk::iterator) {
                String name = path.getFileName() == null ? "" : path.getFileName().toString();
                if (name.startsWith("COBBLE-SNAPSHOT-") && name.endsWith("-MANIFEST")) {
                    operatorId =
                            name.substring(
                                    "COBBLE-SNAPSHOT-".length(),
                                    name.length() - "-MANIFEST".length());
                    Path chkDir = path.getParent();
                    String chkName =
                            chkDir.getFileName() == null ? "" : chkDir.getFileName().toString();
                    if (chkName.startsWith("chk-")) {
                        try {
                            long id = Long.parseLong(chkName.substring("chk-".length()));
                            if (id > latestCheckpointId) {
                                latestCheckpointId = id;
                                checkpointDir = chkDir.getParent();
                            }
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
            }
        }
        assertTrue(operatorId != null, "no COBBLE-SNAPSHOT manifest found");
        assertTrue(latestCheckpointId > 0, "no chk-* checkpoint found");
        assertTrue(checkpointDir != null, "could not resolve checkpoint root");
        return Tuple2.of(operatorId + "@" + checkpointDir.toUri().toString(), latestCheckpointId);
    }

    private static long resolveSchemaCheckpointId(String checkpointRootUri, String operatorId)
            throws Exception {
        Path eventsDir =
                java.nio.file.Paths.get(java.net.URI.create(checkpointRootUri))
                        .resolve("cobble")
                        .resolve(operatorId)
                        .resolve("inspect-schema")
                        .resolve("events");
        long best = -1L;
        try (Stream<Path> walk = Files.list(eventsDir)) {
            for (Path path : (Iterable<Path>) walk::iterator) {
                InspectSchemaRegistryLayout.SchemaEvent event =
                        InspectSchemaRegistryLayout.parseEventFileName(
                                path.getFileName().toString());
                if (event != null && event.checkpointId() > best) {
                    best = event.checkpointId();
                }
            }
        }
        assertTrue(best > 0, "no inspect-schema event found");
        return best;
    }

    private static void waitForCompletedCheckpoint(
            MiniCluster miniCluster, JobID jobId, long checkpointId, Duration timeout)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            org.apache.flink.runtime.executiongraph.AccessExecutionGraph graph =
                    miniCluster.getExecutionGraph(jobId).get();
            JobStatus status = graph.getState();
            if (status.isGloballyTerminalState()) {
                throw new AssertionError(
                        "Job "
                                + jobId
                                + " entered terminal state "
                                + status
                                + " before checkpoint "
                                + checkpointId
                                + " completed.");
            }
            org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot stats =
                    graph.getCheckpointStatsSnapshot();
            if (stats != null
                    && stats.getHistory() != null
                    && stats.getHistory().getLatestCompletedCheckpoint() != null
                    && stats.getHistory().getLatestCompletedCheckpoint().getCheckpointId()
                            >= checkpointId) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Timed out waiting for checkpoint "
                        + checkpointId
                        + " on job "
                        + jobId
                        + " after "
                        + timeout
                        + ".");
    }

    private static StateSourceConfig stateSourceConfig(
            String checkpointRootUri,
            String operatorId,
            String stateName,
            String stateKind,
            List<StateSourceField> fields,
            long checkpointId,
            String scanCheckpointId,
            String scanMode)
            throws Exception {
        return new StateSourceConfig(
                checkpointRootUri,
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                operatorId,
                stateName,
                stateKind,
                scanCheckpointId,
                scanMode,
                resolveSchemaCheckpointId(checkpointRootUri, operatorId),
                -1,
                0L,
                fields);
    }

    private static StateSourceConfig stateSourceConfigWithContract(
            String checkpointRootUri,
            String operatorId,
            String stateName,
            String stateKind,
            List<StateSourceField> outputFields,
            List<StateSourceField> requiredFields,
            int[] requiredPositions,
            String scanCheckpointId,
            String scanMode)
            throws Exception {
        StateSourceLookupKeyContract contract =
                StateSourceLookupKeyContract.present(requiredFields, requiredPositions);
        return new StateSourceConfig(
                checkpointRootUri,
                StateSourceConfig.Layout.CHECKPOINT_ROOT,
                operatorId,
                stateName,
                stateKind,
                scanCheckpointId,
                scanMode,
                resolveSchemaCheckpointId(checkpointRootUri, operatorId),
                -1,
                0L,
                outputFields,
                contract);
    }

    private static RowData singleIntRow(int value) {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, value);
        return row;
    }

    private static RowData twoIntRow(int first, int second) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, first);
        row.setField(1, second);
        return row;
    }

    private static final class ContinuousIntegerSource extends RichParallelSourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int nextValue = getRuntimeContext().getIndexOfThisSubtask();
            int step = getRuntimeContext().getNumberOfParallelSubtasks();
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextValue);
                    nextValue += step;
                }
                Thread.sleep(1L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class ValueStateMapper extends RichMapFunction<Integer, Integer> {
        private transient ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "value-state", BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            Integer current = valueState.value();
            valueState.update(current == null ? value : current + value);
            return value;
        }
    }

    private static final class ReducingStateMapper extends RichMapFunction<Integer, Integer> {
        private transient ReducingState<Integer> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            reducingState =
                    getRuntimeContext()
                            .getReducingState(
                                    new ReducingStateDescriptor<>(
                                            "reducing-state",
                                            (a, b) -> a + b,
                                            BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            reducingState.add(value);
            return value;
        }
    }

    private static final class AggregatingStateMapper extends RichMapFunction<Integer, Integer> {
        private transient AggregatingState<Integer, Integer> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            aggregatingState =
                    getRuntimeContext()
                            .getAggregatingState(
                                    new AggregatingStateDescriptor<>(
                                            "aggregating-state",
                                            new SumAggregateFunction(),
                                            BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            aggregatingState.add(value);
            return value;
        }
    }

    /** Simple sum aggregate function: accumulator = Integer, output = Integer. */
    private static final class SumAggregateFunction
            implements AggregateFunction<Integer, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Integer value, Integer accumulator) {
            return accumulator + value;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    /**
     * Writes {@code mapState.put(value, value * 10)} keyed by {@code value % PARALLELISM}. The
     * state key is {@code value % 4} (0..3), and the map user key is the raw input value.
     */
    private static final class MapStateMapper extends RichMapFunction<Integer, Integer> {
        private transient MapState<Integer, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "map-state",
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            mapState.put(value, value * 10);
            return value;
        }
    }

    /**
     * Writes {@code mapState.put(value, null)} to produce present-null map entries. The Cobble
     * backend supports present-null encoding, so the entry exists with a null value.
     */
    private static final class NullValueMapStateMapper extends RichMapFunction<Integer, Integer> {
        private transient MapState<Integer, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "map-state",
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            BasicTypeInfo.INT_TYPE_INFO));
        }

        @Override
        public Integer map(Integer value) throws Exception {
            mapState.put(value, null);
            return value;
        }
    }
}
