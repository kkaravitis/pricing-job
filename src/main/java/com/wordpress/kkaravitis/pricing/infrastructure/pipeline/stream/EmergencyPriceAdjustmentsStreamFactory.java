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
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import com.wordpress.kkaravitis.pricing.domain.EmergencyPriceAdjustment;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.OrderEvent;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource.KafkaSourceContext;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Detects flash‐sale spikes (≥10 orders in 1 min) and emits EmergencyPriceAdjustment.
 */
public class EmergencyPriceAdjustmentsStreamFactory {

    public DataStream<MetricUpdate> build(StreamExecutionEnvironment env, Configuration config) {

        CommonKafkaSource<OrderEvent> ordersKafkaSource =
              new CommonKafkaSource<>(KafkaSourceContext.<OrderEvent>builder()
                    .sourceId("OrderEvents")
                    .groupId(config.get(PricingConfigOptions.KAFKA_ORDERS_GROUP_ID))
                    .messageType(OrderEvent.class)
                    .topic(config.get(PricingConfigOptions.KAFKA_ORDERS_TOPIC))
                    .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .watermarkStrategySupplier(WatermarkStrategy::forMonotonousTimestamps)
                    .bounded(config.get(PricingConfigOptions.TEST_MODE))
              .build());


        DataStream<OrderEvent> orderEvents = ordersKafkaSource.create(env);

        // 2) Define a CEP pattern: ten or more events in 1 minute
        Pattern<OrderEvent, ?> flashSalePattern = Pattern.<OrderEvent>begin("start")
              .where(new SimpleCondition<>() {
                  @Override
                  public boolean filter(OrderEvent value) {
                      return true;
                  }
              })
              .times(10)                            // require at least 10 matches
              .consecutive()                       // back‐to‐back
              .within(Duration.ofMinutes(1));      // within one minute

        // 3) Apply pattern keyed by productId
        SingleOutputStreamOperator<EmergencyPriceAdjustment> adjustments =
              CEP.pattern(
                          orderEvents.keyBy(OrderEvent::productId),
                          flashSalePattern
                    )
                    .select((PatternSelectFunction<OrderEvent, EmergencyPriceAdjustment>) pattern -> {
                        List<OrderEvent> events = pattern.get("start");
                        OrderEvent orderEvent = events.get(0);
                        // e.g. increase price by 20% during flash sale
                        return new EmergencyPriceAdjustment(orderEvent.productId(), orderEvent.productName(), 1.2);
                    });


        return adjustments
              .map(a -> new MetricUpdate(a.productId(), MetricType.EMERGENCY, a))
              .name("EmergencyAdjustmentUpdate");
    }
}
