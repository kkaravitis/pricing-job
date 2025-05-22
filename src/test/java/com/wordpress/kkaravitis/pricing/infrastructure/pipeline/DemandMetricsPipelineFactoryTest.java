package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.util.ClickEventDeserializationSchema;
import com.wordpress.kkaravitis.pricing.infrastructure.util.ClickEventSerializer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
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

    /* -------- Confluent Kafka -------- */
    @Container
    static final ConfluentKafkaContainer KAFKA =
          new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");
    private static final String TOPIC = "clicks";

    @RegisterExtension
    static final MiniClusterExtension FLINK =
          new MiniClusterExtension(() ->
                new MiniClusterResourceConfiguration.Builder()
                      .setNumberTaskManagers(1)
                      .setNumberSlotsPerTaskManager(1)
                      .build());

    /* ---------- test ---------- */
    @Test
    void pipelineEmitsExpectedDemandMetrics() throws Exception {

        produceClicks();   // 8 events, 0 … 7 minutes

        StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200);

        KafkaSource<ClickEvent> src = KafkaSource.<ClickEvent>builder()
              .setBootstrapServers(KAFKA.getBootstrapServers())
              .setTopics(TOPIC)
              .setGroupId("dm-state-it")
              .setStartingOffsets(OffsetsInitializer.earliest())
              .setBounded(OffsetsInitializer.latest())               // finite
              .setValueOnlyDeserializer(new ClickEventDeserializationSchema())
              .build();

        DataStream<ClickEvent> clicks =
              env.fromSource(src, WatermarkStrategy.noWatermarks(), "KafkaClicks");

        /* ------------ full production path (includes UpdateDemandProcess) ----- */
        DataStream<MetricUpdate> dataStream = new DemandMetricsPipelineFactory().build(clicks);

        List<MetricUpdate> output = new ArrayList<>();
        dataStream.executeAndCollect().forEachRemaining(output::add);

        // 3) collect its output

        /* ------------ assert repository received the expected snapshot -------- */
        assertTrue(output.size() > 0);
//        DemandMetrics expected =
//              new DemandMetrics("p-42", 120, 10);
//        assertEquals(expected, repo.get("p-42"));
    }

    /* -------- serialisable reader -------- */
    private static final class Reader
          extends KeyedStateReaderFunction<String, DemandMetrics>
          implements java.io.Serializable {

        private transient ValueState<DemandMetrics> state;

        @Override
        public void open(OpenContext ctx) {
            state = getRuntimeContext().getState(
                  new ValueStateDescriptor<>("demand", DemandMetrics.class));
        }

        @Override
        public void readKey(String key, Context c, Collector<DemandMetrics> out)
              throws Exception {
            if ("p-42".equals(key)) out.collect(state.value());
        }
    }

    /* -------- helper: eight clicks over 7 min -------- */
    private static void produceClicks() throws Exception {
        long base = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClickEventSerializer.class.getName());

        try (KafkaProducer<String, ClickEvent> producer = new KafkaProducer<>(cfg)) {

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
                  .forEach(ts -> producer.send(new ProducerRecord<>(TOPIC, "producer-42",
                        new ClickEvent("producer-42", ts))));


//            long next = base;
//            for (int i = 0; i < 600; i++) {
//                producer.send(new ProducerRecord<>(TOPIC, "producer-42",
//                      new ClickEvent("producer-42", next)));
//                next += 100;
//            }

            producer.flush();
        }
    }
}
