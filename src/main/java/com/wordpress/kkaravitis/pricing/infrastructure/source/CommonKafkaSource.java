package com.wordpress.kkaravitis.pricing.infrastructure.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CommonKafkaSource<T> {
    private final ObjectMapper mapper;
    private final KafkaSourceContext<T> context;

    public CommonKafkaSource(KafkaSourceContext<T> context) {
        this.mapper = new ObjectMapper();
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
              .map(json -> mapper.readValue(json, context.messageType));
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

}
