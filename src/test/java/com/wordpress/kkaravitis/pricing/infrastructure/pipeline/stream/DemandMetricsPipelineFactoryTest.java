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
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.DemandMetricsStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource.KafkaSourceContext;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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


@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class DemandMetricsPipelineFactoryTest {

    @Container
    static final ConfluentKafkaContainer KAFKA =
          new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    private static final String TOPIC = "clicks";
    private static final ObjectMapper mapper = new ObjectMapper();


    @RegisterExtension
    static final MiniClusterExtension FLINK =
          new MiniClusterExtension(() ->
                new MiniClusterResourceConfiguration.Builder()
                      .setNumberTaskManagers(1)
                      .setNumberSlotsPerTaskManager(1)
                      .build());

    @Test
    void pipelineEmitsExpectedDemandMetrics() throws Exception {

        // given
        produceClicks();   // 8 events, 0 … 7 minutes

        StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(200);

        DataStream<ClickEvent> clicks =
              new CommonKafkaSource<>(KafkaSourceContext.<ClickEvent>builder()
                    .sourceId("clickEvents")
                    .groupId("clicks-events")
                    .messageType(ClickEvent.class)
                    .topic(TOPIC)
                    .brokers(KAFKA.getBootstrapServers())
                    .watermarkStrategySupplier(WatermarkStrategy::noWatermarks)
                    .bounded(true)
                    .build()).create(env);

        DataStream<MetricUpdate> dataStream = new DemandMetricsStreamFactory().build(clicks);

        List<MetricUpdate> output = new ArrayList<>();

        // when
        dataStream.executeAndCollect().forEachRemaining(output::add);

        // then
        assertTrue(output.size() > 0);
        assertEquals(MetricType.DEMAND, output.get(0).type());
        assertTrue(output.get(0).payload() instanceof DemandMetrics);
        DemandMetrics demandMetrics = (DemandMetrics)output.get(0).payload();
        assertEquals(2, demandMetrics.currentDemand());
    }

    private  void produceClicks() throws Exception {
        long base = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {

            Stream.of(base,
                             base + 5_000L,
                             base + 10_000L,
                             base + 15000L,
                             base + 20000L,
                             base + 25000L,
                             base + 30000,
                             base + 35000L,
                             base + 40000,
                             base + 43000)
                  .forEach(ts -> {
                      try {
                          producer.send(new ProducerRecord<>(TOPIC, "producer-42",
                                mapper.writeValueAsString(new ClickEvent("producer-42", "p-42", ts))));
                      } catch (JsonProcessingException e) {
                          throw new RuntimeException(e);
                      }
                  });

            producer.flush();
        }
    }
}
