package com.mycompany.pricing.infrastructure.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Reads serialized ML model bytes from Kafka and broadcasts them to all downstream tasks.
 */
public class KafkaModelBroadcastSource {

    /** Descriptor for the broadcast state holding ML model bytes. */
    public static final MapStateDescriptor<String, byte[]> MODEL_DESCRIPTOR =
          new MapStateDescriptor<>(
                "model-bytes",
                String.class,
                byte[].class
          );

    private final KafkaSource<byte[]> kafkaSource;

    /**
     * @param brokers Kafka bootstrap servers
     * @param topic   Kafka topic carrying serialized model messages
     * @param groupId Consumer group ID
     */
    public KafkaModelBroadcastSource(String brokers, String topic, String groupId) {
        this.kafkaSource = KafkaSource.<byte[]>builder()
              .setBootstrapServers(brokers)
              .setTopics(topic)
              .setGroupId(groupId)
              .setStartingOffsets(OffsetsInitializer.latest())
              // Use Flink's ByteArrayDeserializationSchema
              .setValueOnlyDeserializer(new RawByteDeserializationSchema())
              .build();
    }

    /**
     * Returns the raw DataStream of model bytes (unbroadcast).
     */
    public DataStream<byte[]> createRaw(StreamExecutionEnvironment env) {
        return env
              .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaModelSource");
    }

    /**
     * Builds and returns a BroadcastStream of model bytes under MODEL_DESCRIPTOR.
     */
    public BroadcastStream<byte[]> create(StreamExecutionEnvironment env) {
        DataStream<byte[]> raw = createRaw(env);
        return raw.broadcast(MODEL_DESCRIPTOR);
    }
}