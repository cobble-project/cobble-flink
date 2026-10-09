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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.WorkloadParams;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

class StateBenchmarkBaseTest {
    private final Blackhole blackhole =
            new Blackhole(
                    "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");

    @Test
    void singleParameterReadUsesFixtureWithoutIteratingReturnedValue() throws Exception {
        Fixture fixture = configured("read");
        assertTrue(fixture.readDuringSetup);
        fixture.readDuringSetup(41, 17, blackhole);
        fixture.readDuringSetup(42, 18, blackhole);
        assertEquals(2, fixture.calls);
        assertEquals(42, fixture.lastFixture.setUpKey);
        assertEquals(18, fixture.lastFixture.mapKey);
    }

    @Test
    void voidReadReceivesBlackhole() throws Exception {
        Fixture fixture = configured("readWithBlackhole");
        fixture.readDuringSetup(41, 17, blackhole);
        assertSame(blackhole, fixture.lastBlackhole);
        assertEquals(1, fixture.calls);
    }

    @Test
    void writeBenchmarkIsNeverInvokedDuringSetup() throws Exception {
        Fixture fixture = configured("write");
        assertFalse(fixture.readDuringSetup);
        fixture.readDuringSetup(41, 17, blackhole);
        assertEquals(0, fixture.calls);
    }

    @Test
    void invocationPreservesOriginalExceptionAndError() {
        Fixture fixture = configured("failedRead");
        assertSame(
                fixture.failure,
                assertThrows(IOException.class, () -> fixture.readDuringSetup(1, 2, blackhole)));
        fixture.configureBenchmark(params("failedError"));
        assertSame(
                fixture.error,
                assertThrows(AssertionError.class, () -> fixture.readDuringSetup(1, 2, blackhole)));
    }

    @Test
    void unsupportedMissingAndAmbiguousMethodsDisableSetupReads() throws Exception {
        for (String name : new String[] {"unsupported", "absent", "overloaded"}) {
            Fixture fixture = configured("read");
            fixture.configureBenchmark(params(name));
            assertFalse(fixture.readDuringSetup);
            fixture.readDuringSetup(41, 17, blackhole);
            assertEquals(0, fixture.calls);
        }
    }

    private static Fixture configured(String name) {
        Fixture fixture = new Fixture();
        fixture.configureBenchmark(params(name));
        return fixture;
    }

    private static BenchmarkParams params(String name) {
        return new BenchmarkParams(
                Fixture.class.getName() + "." + name,
                "",
                false,
                1,
                new int[] {1},
                Collections.emptyList(),
                0,
                0,
                null,
                null,
                Mode.Throughput,
                new WorkloadParams(),
                TimeUnit.MILLISECONDS,
                1,
                "",
                Collections.emptyList(),
                "",
                "",
                "",
                "",
                TimeValue.seconds(1));
    }

    public static class Fixture extends StateBenchmarkBase {
        final IOException failure = new IOException("read failed");
        final AssertionError error = new AssertionError("read failed");
        int calls;
        KeyValue lastFixture;
        Blackhole lastBlackhole;

        @ReadOperation
        public Iterable<Long> read(KeyValue keyValue) {
            if (lastFixture != null) {
                assertSame(lastFixture, keyValue);
            }
            calls++;
            lastFixture = keyValue;
            return () -> {
                throw new AssertionError("setup must not iterate a returned iterable");
            };
        }

        @ReadOperation
        public void readWithBlackhole(KeyValue keyValue, Blackhole bh) {
            calls++;
            lastFixture = keyValue;
            lastBlackhole = bh;
            bh.consume(keyValue.setUpKey);
        }

        public void write(KeyValue keyValue) {
            calls++;
        }

        @ReadOperation
        public Long failedRead(KeyValue keyValue) throws IOException {
            throw failure;
        }

        @ReadOperation
        public Long failedError(KeyValue keyValue) {
            throw error;
        }

        @ReadOperation
        public void unsupported(String value) {}

        public void overloaded(KeyValue keyValue) {}

        public void overloaded(KeyValue keyValue, Blackhole bh) {}
    }
}
