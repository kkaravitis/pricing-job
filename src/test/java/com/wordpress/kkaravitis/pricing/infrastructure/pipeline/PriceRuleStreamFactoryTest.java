package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PriceRule;
import com.wordpress.kkaravitis.pricing.domain.PriceRuleUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class PriceRuleStreamFactoryTest {
    @Container
    static final ConfluentKafkaContainer KAFKA =
          new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    static final String TOPIC = "priceRule";

    static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<PriceRuleUpdate> priceRuleUpdates = List
          .of(new PriceRuleUpdate("p-1", new PriceRule(new Money(10, "USD"),new Money(30, "USD"))),
                new PriceRuleUpdate("p-2", new PriceRule(new Money(15, "USD"),new Money(40, "USD"))),
                new PriceRuleUpdate("p-3", new PriceRule(new Money(115, "USD"),new Money(140, "USD"))));

    @RegisterExtension
    static final MiniClusterExtension FLINK =
          new MiniClusterExtension(() ->
                new MiniClusterResourceConfiguration.Builder()
                      .setNumberTaskManagers(1)
                      .setNumberSlotsPerTaskManager(1)
                      .build());

    @Test
    void testPipeline() throws Exception {
        // given
        producePriceRuleUpdateMessages();

        StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200);

        Configuration config = new Configuration();
        config.set(PricingConfigOptions.KAFKA_PRICERULE_GROUP_ID, TOPIC + "-GRP");
        config.set(PricingConfigOptions.KAFKA_PRICERULE_TOPIC, TOPIC);
        config.set(PricingConfigOptions.TEST_MODE, true);
        config.set(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS, KAFKA.getBootstrapServers());

        List<MetricUpdate> output = new ArrayList<>();

        DataStream<MetricUpdate> dataStream = new PriceRuleStreamFactory().build(env, config);

        // when
        dataStream.executeAndCollect().forEachRemaining(output::add);

        // then
        assertEquals(priceRuleUpdates.size(), output.size());
    }


    private void producePriceRuleUpdateMessages() {
        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(cfg)) {

            priceRuleUpdates
                  .forEach(priceRuleUpdate -> {
                      try {
                          producer.send(new ProducerRecord<>(
                                TOPIC,
                                "producer-42",
                                MAPPER.writeValueAsString(priceRuleUpdate)));
                      } catch (JsonProcessingException exception) {
                          throw new RuntimeException(exception);
                      }
                  });

            producer.flush();
        }
    }

}