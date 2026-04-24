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

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.util.Locale;

/** Entry point for running the state benchmark suite with optional local overrides. */
public final class StateBenchmarkSuite {
    private StateBenchmarkSuite() {}

    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder optionsBuilder =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + ValueStateBenchmark.class.getCanonicalName() + ".*")
                        .include(".*" + MapStateBenchmark.class.getCanonicalName() + ".*")
                        .include(".*" + ListStateBenchmark.class.getCanonicalName() + ".*")
                        .shouldFailOnError(true);

        Integer warmupIterations = integerProperty("cobble.state.bench.warmup.iterations");
        if (warmupIterations != null) {
            optionsBuilder.warmupIterations(warmupIterations);
        }

        Integer measurementIterations =
                integerProperty("cobble.state.bench.measurement.iterations");
        if (measurementIterations != null) {
            optionsBuilder.measurementIterations(measurementIterations);
        }

        Integer forks = integerProperty("cobble.state.bench.forks");
        if (forks != null) {
            optionsBuilder.forks(forks);
        }

        Long iterationSeconds = longProperty("cobble.state.bench.iteration.seconds");
        if (iterationSeconds != null) {
            optionsBuilder.warmupTime(TimeValue.seconds(iterationSeconds));
            optionsBuilder.measurementTime(TimeValue.seconds(iterationSeconds));
        }

        String resultPath = System.getProperty("cobble.state.bench.result");
        if (resultPath != null) {
            File resultFile = new File(resultPath);
            File resultDirectory = resultFile.getParentFile();
            if (resultDirectory != null) {
                resultDirectory.mkdirs();
            }
            optionsBuilder.result(resultFile.getAbsolutePath());

            String resultFormat =
                    System.getProperty("cobble.state.bench.result.format", "csv")
                            .toUpperCase(Locale.ROOT);
            optionsBuilder.resultFormat(ResultFormatType.valueOf(resultFormat));
        }

        Options options = optionsBuilder.build();

        new Runner(options).run();
    }

    private static Integer integerProperty(String key) {
        String value = System.getProperty(key);
        return value == null ? null : Integer.valueOf(value);
    }

    private static Long longProperty(String key) {
        String value = System.getProperty(key);
        return value == null ? null : Long.valueOf(value);
    }
}
