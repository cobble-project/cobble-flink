/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cobble.flink.state.benchmark;

import io.cobble.flink.state.CobbleOptions;
import io.cobble.flink.state.CobbleStateBackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.TestTaskStateManagerBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.FileUtils;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Statistics;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;

/** Local replacement for the upstream benchmark helper utilities. */
public final class StateBackendBenchmarkUtils {
    private static final JobVertexID BENCHMARK_JOB_VERTEX_ID =
            JobVertexID.fromHexString("22222222222222222222222222222222");
    private static final org.apache.flink.runtime.state.KeyGroupRange KEY_GROUP_RANGE =
            org.apache.flink.runtime.state.KeyGroupRange.of(0, 1);
    private static final int NUMBER_OF_KEY_GROUPS = 2;
    private static final String OPERATOR_IDENTIFIER = "state-benchmark";
    private static volatile Statistics lastRocksDbStatistics;

    public enum StateBackendType {
        HEAP,
        ROCKSDB,
        COBBLE
    }

    public static BenchmarkBackend createKeyedStateBackend(StateBackendType backendType)
            throws Exception {
        File benchmarkRoot = prepareDirectory("backend-root");
        File workingDir = new File(benchmarkRoot, "tm-working-dir");
        Files.createDirectories(workingDir.toPath());

        MockEnvironment environment = createEnvironment(workingDir);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        AbstractKeyedStateBackend<Long> keyedStateBackend = null;
        try {
            switch (backendType) {
                case HEAP:
                    keyedStateBackend =
                            createHeapBackend(environment, cancelStreamRegistry, benchmarkRoot);
                    break;
                case ROCKSDB:
                    keyedStateBackend =
                            createRocksDbBackend(environment, cancelStreamRegistry, benchmarkRoot);
                    break;
                case COBBLE:
                    keyedStateBackend =
                            createCobbleBackend(environment, cancelStreamRegistry, benchmarkRoot);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported state backend: " + backendType);
            }
            return new BenchmarkBackend(
                    benchmarkRoot,
                    workingDir,
                    environment,
                    cancelStreamRegistry,
                    keyedStateBackend);
        } catch (Exception e) {
            if (keyedStateBackend != null) {
                keyedStateBackend.close();
                keyedStateBackend.dispose();
            }
            environment.close();
            cancelStreamRegistry.close();
            deleteQuietly(benchmarkRoot);
            throw e;
        }
    }

    public static <T> ValueState<T> getValueState(
            AbstractKeyedStateBackend<Long> keyedStateBackend, ValueStateDescriptor<T> descriptor)
            throws Exception {
        return keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
    }

    public static <UK, UV> MapState<UK, UV> getMapState(
            AbstractKeyedStateBackend<Long> keyedStateBackend,
            MapStateDescriptor<UK, UV> descriptor)
            throws Exception {
        return keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
    }

    public static <T> ListState<T> getListState(
            AbstractKeyedStateBackend<Long> keyedStateBackend, ListStateDescriptor<T> descriptor)
            throws Exception {
        return keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
    }

    public static <S extends State, T> void applyToAllKeys(
            AbstractKeyedStateBackend<Long> keyedStateBackend,
            StateDescriptor<S, T> descriptor,
            KeyedStateFunction<Long, S> function)
            throws Exception {
        keyedStateBackend.applyToAllKeys(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor, function);
    }

    public static <S extends State, T> boolean compactState(
            AbstractKeyedStateBackend<Long> keyedStateBackend, StateDescriptor<S, T> descriptor)
            throws Exception {
        Method compactState;
        try {
            compactState =
                    keyedStateBackend.getClass().getMethod("compactState", StateDescriptor.class);
        } catch (NoSuchMethodException ignored) {
            return false;
        }
        try {
            compactState.invoke(keyedStateBackend, descriptor);
            return true;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Failed to access compactState on benchmark backend.", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException("Failed to compact benchmark backend state.", cause);
        }
    }

    public static void cleanUp(BenchmarkBackend benchmarkBackend) throws Exception {
        if (benchmarkBackend == null) {
            return;
        }
        Exception failure = null;
        try {
            benchmarkBackend.keyedStateBackend.close();
        } catch (Exception e) {
            failure = e;
        }
        try {
            benchmarkBackend.keyedStateBackend.dispose();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            benchmarkBackend.cancelStreamRegistry.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            benchmarkBackend.environment.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            deleteQuietly(benchmarkBackend.benchmarkRoot);
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    public static File prepareDirectory(String prefix) throws IOException {
        File root = resolveBenchmarkDataRoot();
        Files.createDirectories(root.toPath());
        return Files.createTempDirectory(root.toPath(), prefix + "-").toFile();
    }

    private static File resolveBenchmarkDataRoot() {
        String configured = System.getProperty("benchmark.state.data-dir");
        if (configured == null || configured.isEmpty()) {
            configured = System.getProperty("cobble.state.bench.data-dir");
        }
        return configured == null ? new File("target/benchmark-state") : new File(configured);
    }

    private static MockEnvironment createEnvironment(File workingDir) {
        return new MockEnvironmentBuilder()
                .setTaskName("state-benchmark-task")
                .setJobVertexID(BENCHMARK_JOB_VERTEX_ID)
                .setManagedMemorySize(MemorySize.ofMebiBytes(256).getBytes())
                .setTaskManagerRuntimeInfo(
                        new TestingTaskManagerRuntimeInfo(new Configuration(), workingDir))
                .setTaskStateManager(new TestTaskStateManagerBuilder().build())
                .build();
    }

    private static AbstractKeyedStateBackend<Long> createHeapBackend(
            MockEnvironment environment, CloseableRegistry cancelStreamRegistry, File benchmarkRoot)
            throws Exception {
        HashMapStateBackend backend = new HashMapStateBackend();
        return backend.createKeyedStateBackend(
                environment,
                resolveJobId(environment),
                OPERATOR_IDENTIFIER,
                LongSerializer.INSTANCE,
                NUMBER_OF_KEY_GROUPS,
                KEY_GROUP_RANGE,
                resolveKvStateRegistry(environment),
                TtlTimeProvider.DEFAULT,
                resolveMetricGroup(environment),
                Collections.<KeyedStateHandle>emptyList(),
                cancelStreamRegistry);
    }

    private static AbstractKeyedStateBackend<Long> createRocksDbBackend(
            MockEnvironment environment, CloseableRegistry cancelStreamRegistry, File benchmarkRoot)
            throws Exception {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setDbStoragePath(new File(benchmarkRoot, "rocksdb").getAbsolutePath());
        if (Boolean.getBoolean("cobble.state.bench.enable-rocksdb-statistics")) {
            backend.setRocksDBOptions(
                    new RocksDBOptionsFactory() {
                        @Override
                        public DBOptions createDBOptions(
                                DBOptions currentOptions,
                                Collection<AutoCloseable> handlesToClose) {
                            Statistics statistics = new Statistics();
                            handlesToClose.add(statistics);
                            lastRocksDbStatistics = statistics;
                            return currentOptions.setStatistics(statistics);
                        }

                        @Override
                        public ColumnFamilyOptions createColumnOptions(
                                ColumnFamilyOptions currentOptions,
                                Collection<AutoCloseable> handlesToClose) {
                            return currentOptions;
                        }
                    });
        } else {
            lastRocksDbStatistics = null;
        }
        return backend.createKeyedStateBackend(
                environment,
                resolveJobId(environment),
                OPERATOR_IDENTIFIER,
                LongSerializer.INSTANCE,
                NUMBER_OF_KEY_GROUPS,
                KEY_GROUP_RANGE,
                resolveKvStateRegistry(environment),
                TtlTimeProvider.DEFAULT,
                resolveMetricGroup(environment),
                Collections.<KeyedStateHandle>emptyList(),
                cancelStreamRegistry);
    }

    private static AbstractKeyedStateBackend<Long> createCobbleBackend(
            MockEnvironment environment, CloseableRegistry cancelStreamRegistry, File benchmarkRoot)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                CobbleOptions.LOCAL_DIRECTORIES,
                new File(benchmarkRoot, "cobble-local").getAbsolutePath());
        configuration.set(CobbleOptions.MEMTABLE_TYPE, "skiplist");
        String configuredLogLevel = System.getProperty("state.backend.cobble.log.level");
        if (configuredLogLevel != null && !configuredLogLevel.isEmpty()) {
            configuration.set(CobbleOptions.LOG_LEVEL, configuredLogLevel);
        }

        CobbleStateBackend backend =
                new CobbleStateBackend()
                        .configure(
                                configuration, StateBackendBenchmarkUtils.class.getClassLoader());

        return backend.createKeyedStateBackend(
                environment,
                resolveJobId(environment),
                OPERATOR_IDENTIFIER,
                LongSerializer.INSTANCE,
                NUMBER_OF_KEY_GROUPS,
                KEY_GROUP_RANGE,
                resolveKvStateRegistry(environment),
                TtlTimeProvider.DEFAULT,
                resolveMetricGroup(environment),
                Collections.<KeyedStateHandle>emptyList(),
                cancelStreamRegistry,
                1.0d);
    }

    private static JobID resolveJobId(MockEnvironment environment) {
        return environment.getJobID();
    }

    private static MetricGroup resolveMetricGroup(MockEnvironment environment) {
        return environment.getMetricGroup();
    }

    private static TaskKvStateRegistry resolveKvStateRegistry(MockEnvironment environment) {
        return environment.getTaskKvStateRegistry();
    }

    private static void deleteQuietly(File directory) throws IOException {
        if (directory != null && directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    public static final class BenchmarkBackend {
        private final File benchmarkRoot;
        private final File workingDir;
        private final MockEnvironment environment;
        private final CloseableRegistry cancelStreamRegistry;
        private final AbstractKeyedStateBackend<Long> keyedStateBackend;

        private BenchmarkBackend(
                File benchmarkRoot,
                File workingDir,
                MockEnvironment environment,
                CloseableRegistry cancelStreamRegistry,
                AbstractKeyedStateBackend<Long> keyedStateBackend) {
            this.benchmarkRoot = benchmarkRoot;
            this.workingDir = workingDir;
            this.environment = environment;
            this.cancelStreamRegistry = cancelStreamRegistry;
            this.keyedStateBackend = keyedStateBackend;
        }

        public File getWorkingDir() {
            return workingDir;
        }

        public File getBenchmarkRoot() {
            return benchmarkRoot;
        }

        public AbstractKeyedStateBackend<Long> getKeyedStateBackend() {
            return keyedStateBackend;
        }
    }

    public static Statistics getLastRocksDbStatistics() {
        return lastRocksDbStatistics;
    }
}
