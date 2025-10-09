/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.OrderEvent;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class EmergencyPriceAdjustmentsStreamFactoryTest {
    @Container
    static final ConfluentKafkaContainer KAFKA =
          new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    static final String TOPIC = "orders";

    static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<OrderEvent> orderEvents = new ArrayList<>();

    @RegisterExtension
    static final MiniClusterExtension FLINK =
          new MiniClusterExtension(() ->
                new MiniClusterResourceConfiguration.Builder()
                      .setNumberTaskManagers(1)
                      .setNumberSlotsPerTaskManager(1)
                      .build());

    @BeforeEach
    void setOrders() {
        Instant base = Instant.parse("2025-01-01T00:00:00Z");

        for (int i = 1; i < 11; i++) {
            orderEvents.add(new OrderEvent(
                  "order-" + i,
                  "best-seller",
                  "best-seller",
                  5,
                  base.plusSeconds(i).toEpochMilli()));
        }
    }

    @Test
    void testPipeline() throws Exception {
        // given
        sendOrders();

        StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200);

        Configuration config = new Configuration();
        config.set(PricingConfigOptions.KAFKA_ORDERS_GROUP_ID, TOPIC + "-GRP");
        config.set(PricingConfigOptions.KAFKA_ORDERS_TOPIC, TOPIC);
        config.set(PricingConfigOptions.TEST_MODE, true);
        config.set(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS, KAFKA.getBootstrapServers());

        List<MetricUpdate> output = new ArrayList<>();

        DataStream<MetricUpdate> dataStream = new EmergencyPriceAdjustmentsStreamFactory().build(env, config);

        // when
        dataStream.executeAndCollect().forEachRemaining(output::add);

        // then
        assertTrue(output.size() > 0);
        assertEquals(1, output.size());
    }

    private void sendOrders() {
        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {
            orderEvents
                  .forEach(orderEvent -> {
                      try {
                          producer.send(new ProducerRecord<>(TOPIC, "order",
                                MAPPER.writeValueAsString(orderEvent)));
                      } catch (JsonProcessingException e) {
                          throw new RuntimeException(e);
                      }
                  });

            producer.flush();
        }

    }

}