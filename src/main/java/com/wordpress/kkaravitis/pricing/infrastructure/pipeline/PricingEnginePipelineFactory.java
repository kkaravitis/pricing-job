/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.MetricOrClick;
import com.wordpress.kkaravitis.pricing.domain.MetricOrClick.Click;
import com.wordpress.kkaravitis.pricing.domain.MetricOrClick.Metric;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource.KafkaModelBroadcastSourceContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

public class PricingEnginePipelineFactory {

    /**
     * side-output tag for large price jumps
     **/
    public static final OutputTag<PricingResult> ALERT_TAG =
          new OutputTag<PricingResult>("price-alerts") {
          };

    public void build(DataStream<ClickEvent> clicks,
          StreamExecutionEnvironment env,
          Configuration config,
          DataStream<MetricUpdate> metricsUnion) {

        KafkaModelBroadcastSource modelBroadcast =
              new KafkaModelBroadcastSource(KafkaModelBroadcastSourceContext.builder()
                    .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .topic(config.get(PricingConfigOptions.KAFKA_MODEL_TOPIC))
                    .groupId(config.get(PricingConfigOptions.KAFKA_MODEL_GROUP_ID))
                    .build());

        // 1) wrap clicks as MetricOrClick.Click and metrics as MetricOrClick.Metric
        DataStream<MetricOrClick> clicksWrapped = clicks
              .map((MapFunction<ClickEvent, MetricOrClick>) Click::new)
              .returns(Types.GENERIC(MetricOrClick.class));

        DataStream<MetricOrClick> metricsWrapped = metricsUnion
              .map((MapFunction<MetricUpdate, MetricOrClick>) Metric::new)
              .returns(Types.GENERIC(MetricOrClick.class));

        // 2) union the two
        DataStream<MetricOrClick> combined = clicksWrapped.union(metricsWrapped);

        KeyedStream<MetricOrClick, String> keyed = combined
              .keyBy(mc -> {
                  if (mc instanceof MetricOrClick.Click c) {
                      return c.event().productId();
                  } else {
                      return ((MetricOrClick.Metric) mc).update().productId();
                  }
              });

        SingleOutputStreamOperator<PricingResult> priced = keyed
              .connect(modelBroadcast.create(env))
              .process(new UnifiedPricingFunction())
              .name("DynamicPricingUnified");

        DataStream<PricingResult> alerts = priced
              .getSideOutput(ALERT_TAG);

        priced.sinkTo(KafkaSink
                    .<PricingResult>builder()
                    .setBootstrapServers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .setRecordSerializer(
                          KafkaRecordSerializationSchema.<PricingResult>builder()
                                .setTopic(config.get(PricingConfigOptions.KAFKA_PRICING_TOPIC))
                                .setKeySerializationSchema(
                                      (SerializationSchema<PricingResult>) r -> r.productId().getBytes()
                                )
                                .setValueSerializationSchema(new PricingResultJsonPojoSchema())
                                .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setTransactionalIdPrefix(config.get(PricingConfigOptions.KAFKA_PRICING_TXN_ID_PREFIX))
                    .setProperty(ProducerConfig.ACKS_CONFIG, "all")
                    .setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                    .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000")
                    .setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000") // end-to-end send timeout
                    .setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,  "30000")  // single request timeout
                    .setProperty(ProducerConfig.RETRIES_CONFIG,            "2147483647") // let client retry
                    .setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1") // safest with idempotence
                    .build())
              .name("PricingKafkaSink");

        alerts
              .map(alert -> "ALERT: price jump for " + alert.productId() +
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
                    .setProperty(ProducerConfig.ACKS_CONFIG, "all")
                    .setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                    .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000")
                    .setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000") // end-to-end send timeout
                    .setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,  "30000")  // single request timeout
                    .setProperty(ProducerConfig.RETRIES_CONFIG,            "2147483647") // let client retry
                    .setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1") // safest with idempotence
                    .build())
              .name("PricingAlertSink");
    }
}
