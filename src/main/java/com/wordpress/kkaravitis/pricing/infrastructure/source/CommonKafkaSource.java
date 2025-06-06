/*
 * Copyright 2025 Konstantinos Karavitis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wordpress.kkaravitis.pricing.infrastructure.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class CommonKafkaSource<T>  {
    private final KafkaSourceContext<T> context;

    public CommonKafkaSource(KafkaSourceContext<T> context) {
        this.context = context;
    }

    public DataStream<T> create(StreamExecutionEnvironment env) {

        KafkaSourceBuilder<String> kafkaSourceBuilder = KafkaSource.<String>builder()
              .setBootstrapServers(context.brokers)
              .setTopics(context.topic)
              .setGroupId(context.groupId)
              .setValueOnlyDeserializer(new SimpleStringSchema());

        if (context.bounded) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
            kafkaSourceBuilder.setBounded(OffsetsInitializer.latest());
        } else {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
        }

        KafkaSource<String> kafkaSource = kafkaSourceBuilder.build();

        return env.fromSource(kafkaSource,
                    context.watermarkStrategySupplier.get(),
                    context.sourceId)
              .map(new StringToObjectMapFunction<>(context.messageType))
              .returns(context.messageType);
    }

    @Getter
    @Builder
    public static class KafkaSourceContext<T> {
        private String sourceId;
        private Class<T> messageType;
        private String brokers;
        private String topic;
        private String groupId;
        private Supplier<WatermarkStrategy<String>> watermarkStrategySupplier;
        private boolean bounded;
    }

    @RequiredArgsConstructor
    static class StringToObjectMapFunction<T> implements MapFunction<String, T> {
        private static final ObjectMapper mapper = new ObjectMapper();

        private final Class<T> messageType;

        @Override
        public T map(String value) throws Exception {
            return mapper.readValue(value, messageType);
        }
    }

}
