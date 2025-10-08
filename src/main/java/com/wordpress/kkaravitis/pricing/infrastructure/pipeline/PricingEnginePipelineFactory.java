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
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.MetricOrClick;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource.KafkaModelBroadcastSourceContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
              new OutputTag<PricingResult>("price-alerts"){};

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
                  .map(MetricOrClick.Click::new);

            DataStream<MetricOrClick> metricsWrapped = metricsUnion
                  .map(MetricOrClick.Metric::new);

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
                                    result ->  result.productId().getBytes()
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
                        .build())
                  .name("PricingAlertSink");
        }
}
