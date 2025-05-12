package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkEmergencyAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class PricingEnginePipelineFactory {
        public void build(DataStream<ClickEvent> clicks, StreamExecutionEnvironment env) {

            KafkaModelBroadcastSource modelCdc =
                  new KafkaModelBroadcastSource("localhost:9092", "model-topic", "model-group");//TODO: Pass from configuration file

            SingleOutputStreamOperator<PricingResult> priced = clicks
                  .keyBy(ClickEvent::getProductId)
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
                  .setBootstrapServers("localhost:9092")//TODO: Pass from configuration file
                  .setRecordSerializer(
                        KafkaRecordSerializationSchema.<PricingResult>builder()
                              .setTopic("pricing-results")
                              .setKeySerializationSchema(
                                    result ->  result.getProduct().getProductId().getBytes()
                              )
                              .setValueSerializationSchema(
                                    new JsonPojoSchema<>()
                              )
                              .build()
                  )
                  .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                  .setTransactionalIdPrefix("pricing-txn-")
                  .setProperty(ProducerConfig.ACKS_CONFIG, "all")
                  .setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                  .build())
                  .name("PricingKafkaSink");

            // alert sink: you could log, side-output to another Kafka topic, etc.
            alerts
                  .map(alert -> "ALERT: price jump for " + alert.getProduct().getProductId() +
                        " new=" + alert.getNewPrice())
                  .sinkTo(KafkaSink
                        .<String>builder()
                        .setBootstrapServers("localhost:9092")//TODO: Pass from configuration file
                        .setRecordSerializer(
                              KafkaRecordSerializationSchema.builder()
                                    .setTopic("pricing-alerts")
                                    .setKeySerializationSchema(new SimpleStringSchema())
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                        )
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("pricing-alerts-txn-")
                        .build())
                  .name("PricingAlertSink");
        }
}
