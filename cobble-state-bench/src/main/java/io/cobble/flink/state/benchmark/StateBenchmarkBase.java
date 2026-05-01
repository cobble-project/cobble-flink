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

import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.MAP_KEYS;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.MAP_KEY_COUNT;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.MAP_VALUES;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.NEW_KEYS;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.NEW_KEY_COUNT;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.RANDOM_VALUES;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.RANDOM_VALUE_COUNT;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.SETUP_KEYS;
import static io.cobble.flink.state.benchmark.StateBenchmarkConstants.SETUP_KEY_COUNT;

import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/** Base implementation for the state benchmarks. */
public class StateBenchmarkBase extends BenchmarkBase {
    protected static AtomicInteger keyIndex;
    protected final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Param({"HEAP", "ROCKSDB", "COBBLE"})
    protected StateBackendBenchmarkUtils.StateBackendType backendType;

    protected StateBackendBenchmarkUtils.BenchmarkBackend benchmarkBackend;
    protected AbstractKeyedStateBackend<Long> keyedStateBackend;

    protected AbstractKeyedStateBackend<Long> createKeyedStateBackend() throws Exception {
        benchmarkBackend = StateBackendBenchmarkUtils.createKeyedStateBackend(backendType);
        keyedStateBackend = benchmarkBackend.getKeyedStateBackend();
        return keyedStateBackend;
    }

    protected static int getCurrentIndex() {
        int currentIndex = keyIndex.getAndIncrement();
        if (currentIndex == Integer.MAX_VALUE) {
            keyIndex.set(0);
        }
        return currentIndex;
    }

    @TearDown
    public void tearDown() throws Exception {
        StateBackendBenchmarkUtils.cleanUp(benchmarkBackend);
        benchmarkBackend = null;
        keyedStateBackend = null;
    }

    @State(Scope.Thread)
    public static class KeyValue {
        public long newKey;
        public long setUpKey;
        public long mapKey;
        public double mapValue;
        public long value;
        public List<Long> listValue;

        @Setup(Level.Invocation)
        public void kvSetup() {
            int currentIndex = getCurrentIndex();
            setUpKey = SETUP_KEYS.get(currentIndex % SETUP_KEY_COUNT);
            newKey = NEW_KEYS.get(currentIndex % NEW_KEY_COUNT);
            mapKey = MAP_KEYS.get(currentIndex % MAP_KEY_COUNT);
            mapValue = MAP_VALUES.get(currentIndex % MAP_KEY_COUNT);
            value = RANDOM_VALUES.get(currentIndex % RANDOM_VALUE_COUNT);
            listValue =
                    Collections.singletonList(RANDOM_VALUES.get(currentIndex % RANDOM_VALUE_COUNT));
        }

        @TearDown(Level.Invocation)
        public void kvTearDown() {
            listValue = null;
        }
    }
}
