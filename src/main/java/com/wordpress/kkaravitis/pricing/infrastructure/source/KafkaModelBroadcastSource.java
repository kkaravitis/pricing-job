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

import lombok.Builder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reads serialized ML model bytes from Kafka and broadcasts them to all downstream tasks.
 */
public class KafkaModelBroadcastSource {

    /** Descriptor for the broadcast state holding ML model bytes. */
    public static final MapStateDescriptor<Void, byte[]> MODEL_DESCRIPTOR =
          new MapStateDescriptor<>(
                "model-bytes",
                Void.class,
                byte[].class
          );

    private final KafkaSource<byte[]> kafkaSource;

    /**
     * @param context Source context.
     */
    public KafkaModelBroadcastSource(KafkaModelBroadcastSourceContext context) {
        this.kafkaSource = KafkaSource.<byte[]>builder()
              .setBootstrapServers(context.brokers)
              .setTopics(context.topic)
              .setGroupId(context.groupId)
              .setStartingOffsets(OffsetsInitializer.latest())
              // Use Flink's ByteArrayDeserializationSchema
              .setValueOnlyDeserializer(new RawByteDeserializationSchema())
              .build();
    }

    /**
     * Builds and returns a BroadcastStream of model bytes under MODEL_DESCRIPTOR.
     */
    public BroadcastStream<byte[]> create(StreamExecutionEnvironment env) {
        return env
              .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaModelSource")
              .broadcast(MODEL_DESCRIPTOR);
    }

    @Builder
    public static class KafkaModelBroadcastSourceContext {
        private String brokers;
        private String topic;
        private String groupId;
    }
}