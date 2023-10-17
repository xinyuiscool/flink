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

package org.apache.flink.streaming.examples.datagen;

import com.facebook.presto.hive.s3.PrestoS3FileSystem;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.fs.s3presto.FlinkS3PrestoFileSystem;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.logging.log4j.core.config.Configurator;

import java.util.Map;

/** An example for generating specific data per checkpoint with a {@link DataGeneratorSource} . */
public class DataGeneratorPerCheckpoint {

    public static void main(String[] args) throws Exception {
        Configurator.initialize(null, "log4j2.xml");

        Map<String, String> cfg = ImmutableMap.of(
                "s3.endpoint", "http://127.0.0.1:9000",
                "s3.path.style.access", "true",
                "s3.access.key", "minioadmin",
                "s3.secret.key", "minioadmin",
                "state.backend", "rocksdb",
                "state.backend.incremental", "true",
                "state.checkpoints.dir", "s3://flink-bucket/checkpoints",
                "execution.savepoint.path", "s3://flink-bucket/checkpoints/bce0f8ee4522d0d50011b06c402026ca/chk-1"
        );
        Configuration flinkConfig = Configuration.fromMap(cfg);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(3000);
        env.setParallelism(1);

        FileSystem.initialize(flinkConfig, null);

        final String[] elements = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        final int size = elements.length;
        final GeneratorFunction<Long, String> generatorFunction =
                index -> elements[(int) (index % size)];

        final DataGeneratorSource<String> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perCheckpoint(size),
                        Types.STRING);

        final DataStreamSource<String> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        streamSource
                .keyBy(a -> a)
                .flatMap(new Count())
                .print();

        env.execute("Data Generator Source Example");
    }

    static final class Count extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        private transient ValueState<Integer> count;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            int newCount = count.value() + 1;
            count.update(newCount);

            out.collect(new Tuple2<>(value, newCount));
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("count", TypeInformation.of(Integer.class), Integer.valueOf(0));
            count = getRuntimeContext().getState(descriptor);
        }
    }
}
