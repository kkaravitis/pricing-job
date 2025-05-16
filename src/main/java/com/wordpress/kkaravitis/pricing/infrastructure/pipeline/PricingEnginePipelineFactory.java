package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkEmergencyAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource.KafkaModelBroadcastSourceContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;

public class PricingEnginePipelineFactory {
        public void build(DataStream<ClickEvent> clicks, StreamExecutionEnvironment env, Configuration config) {

            KafkaModelBroadcastSource modelCdc =
                  new KafkaModelBroadcastSource(KafkaModelBroadcastSourceContext.builder()
                        .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                        .topic(config.get(PricingConfigOptions.KAFKA_MODEL_TOPIC))
                        .groupId(config.get(PricingConfigOptions.KAFKA_MODEL_GROUP_ID))
                        .build());

            SingleOutputStreamOperator<PricingResult> priced = clicks
                  .keyBy(ClickEvent::productId)
                  .connect(modelCdc.create(env))
                  .process(new PricingWithModelBroadcastFunction(
                        new FlinkPriceRuleRepository(),
                        new FlinkDemandMetricsRepository(),
                        new FlinkInventoryLevelRepository(),
                        new FlinkCompetitorPriceRepository(),
                        new FlinkEmergencyAdjustmentRepository(),
                        new MlModelAdapter()
                  ))
                  .name("DynamicPricingUnified");


            DataStream<PricingResult> alerts = priced
                  .getSideOutput(PricingWithModelBroadcastFunction.ALERT_TAG);


            priced.sinkTo(KafkaSink
                  .<PricingResult>builder()
                  .setBootstrapServers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                  .setRecordSerializer(
                        KafkaRecordSerializationSchema.<PricingResult>builder()
                              .setTopic(config.get(PricingConfigOptions.KAFKA_PRICING_TOPIC))
                              .setKeySerializationSchema(
                                    result ->  result.product().productId().getBytes()
                              )
                              .setValueSerializationSchema(
                                    new JsonPojoSchema<>()
                              )
                              .build()
                  )
                  .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                  .setTransactionalIdPrefix(config.get(PricingConfigOptions.KAFKA_PRICING_TXN_ID_PREFIX))
                  .setProperty(ProducerConfig.ACKS_CONFIG, "all")
                  .setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                  .build())
                  .name("PricingKafkaSink");

            // alert sink: you could log, side-output to another Kafka topic, etc.
            alerts
                  .map(alert -> "ALERT: price jump for " + alert.product().productId() +
                        " new=" + alert.newPrice())
                  .sinkTo(KafkaSink
                        .<String>builder()
                        .setBootstrapServers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                        .setRecordSerializer(
                              KafkaRecordSerializationSchema.builder()
                                    .setTopic(config.get(PricingConfigOptions.KAFKA_ALERTS_TOPIC))
                                    .setKeySerializationSchema(new SimpleStringSchema())
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                        )
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix(config.get(PricingConfigOptions.KAFKA_ALERTS_TXN_ID_PREFIX))
                        .build())
                  .name("PricingAlertSink");
        }
}
