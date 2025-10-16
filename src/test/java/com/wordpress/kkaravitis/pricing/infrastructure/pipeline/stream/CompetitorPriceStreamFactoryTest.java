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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource.KafkaSourceContext;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.CompetitorPriceStreamFactory;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
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
import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class CompetitorPriceStreamFactoryTest {
    @Container
    static final ConfluentKafkaContainer KAFKA =
          new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    @Container
    static final MockServerContainer mockServer = new MockServerContainer(DockerImageName
          .parse("mockserver/mockserver:5.15.0"));

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
    void pipelineEmitsExpectedMetricUpdates() throws Exception {
        final Integer port = mockServer.getServerPort();
        final String host = mockServer.getHost();
        final String baseUrl = String.format("http://%s:%s", host, port);
        Configuration config = new Configuration();
        config.set(PricingConfigOptions.COMPETITOR_API_BASE_URL, baseUrl);

        try (var mockServer = new MockServerClient(host, port)) {
            mockServer.when(HttpRequest.request().withPath("/product-1"))
              .respond(HttpResponse.response()
                    .withStatusCode(200)
                    .withBody("""
              {
                "price" : 42.0
              }
              """));

            produceClicks();

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

            DataStream<MetricUpdate> dataStream = new CompetitorPriceStreamFactory().build(clicks, config);

            List<MetricUpdate> output = new ArrayList<>();

            //when
            dataStream.executeAndCollect().forEachRemaining(output::add);

            //then
            assertEquals(1, output.size());
            Serializable payload = output.get(0).payload();
            assertNotNull(payload);
            assertTrue(payload instanceof CompetitorPrice);
            CompetitorPrice competitorPrice = (CompetitorPrice) payload;
            assertEquals(new Money(42.0, "EUR"), competitorPrice.price());

        }
    }


    private  void produceClicks() {
        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        long baseTimestamp = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {
            try {
                producer.send(new ProducerRecord<>(TOPIC, "producer-43",
                      mapper.writeValueAsString(new ClickEvent(
                            "product-1",
                            "p-1",
                            baseTimestamp))));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            producer.flush();
        }
    }

}