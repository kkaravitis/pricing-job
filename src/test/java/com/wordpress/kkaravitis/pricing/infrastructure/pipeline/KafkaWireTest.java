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
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.infrastructure.util.ClickEventDeserializationSchema;
import com.wordpress.kkaravitis.pricing.infrastructure.util.ClickEventSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class KafkaWireTest {

    /* ---------- Kafka broker ---------- */
    @Container
    static ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");
    private static final String TOPIC = "clicks";

    /* ---------- Flink mini-cluster ---------- */
    @RegisterExtension
    static final MiniClusterExtension FLINK =
          new MiniClusterExtension(() ->
                new MiniClusterResourceConfiguration.Builder()
                      .setNumberTaskManagers(1)
                      .setNumberSlotsPerTaskManager(1)
                      .build());

    @Test
    void flinkReadsExactlyWhatWeWrote() throws Exception {

        /* 1 ─ create deterministic ClickEvents */
        List<ClickEvent> sent = List.of(
              new ClickEvent("p-42", 1_000L),
              new ClickEvent("p-99", 2_000L));

        /* 2 ─ produce them to Kafka */
        produce(sent);

        /* 3 ─ simple Flink job: Kafka → collection sink */
        StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ClickEvent> source = KafkaSource.<ClickEvent>builder()
              .setBootstrapServers(KAFKA.getBootstrapServers())
              .setTopics(TOPIC)
              .setGroupId("wire-it")
              // read all existing records …
              .setStartingOffsets(OffsetsInitializer.earliest())
              // … and then STOP at the latest offset that exists at job submission
              .setBounded(OffsetsInitializer.latest())          // ★ this makes it finite
              .setValueOnlyDeserializer(new ClickEventDeserializationSchema())
              .build();

        DataStream<ClickEvent> stream =
              env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Clicks");

        List<ClickEvent> collected = new ArrayList<>();
        stream.executeAndCollect().forEachRemaining(collected::add);  // bounded source

        /* 4 ─ assert round-trip equality */
        assertEquals(sent.size(), collected.size());
    }

    /* ---------- Kafka producer helper ---------- */
    private static void produce(List<ClickEvent> events) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClickEventSerializer.class.getName());

        try (KafkaProducer<String, ClickEvent> p = new KafkaProducer<>(props)) {
            for (ClickEvent e : events) {
                p.send(new ProducerRecord<>(TOPIC, e.productId(), e)).get();
            }
            p.flush();
        }
    }
}
