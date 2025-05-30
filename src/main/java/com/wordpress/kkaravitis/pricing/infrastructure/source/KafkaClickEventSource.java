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

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reads click events from Kafka and deserializes JSON into ClickEvent objects.
 */
public class KafkaClickEventSource {
    private final KafkaSource<String> kafkaSource;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaClickEventSource(KafkaClickEventSourceContext context) {
        this.kafkaSource = KafkaSource.<String>builder()
              .setBootstrapServers(context.brokers)
              .setTopics(context.topic)
              .setGroupId(context.groupId)
              .setStartingOffsets(OffsetsInitializer.latest())
              .setValueOnlyDeserializer(new SimpleStringSchema())
              .build();
    }

    /**
     * Creates a DataStream of ClickEvent by consuming from Kafka and parsing JSON.
     * @param env Flink execution environment
     * @return DataStream of ClickEvent
     */
    public DataStream<ClickEvent> create(StreamExecutionEnvironment env) {
        return env
              .fromSource(kafkaSource,
                    WatermarkStrategy.forMonotonousTimestamps(),
                    "KafkaClickEventSource")
              .map(json -> mapper.readValue(json, ClickEvent.class));
    }

    @Builder
    @Getter
    public static class KafkaClickEventSourceContext {
        private String brokers;
        private String topic;
        private String groupId;
    }
}
