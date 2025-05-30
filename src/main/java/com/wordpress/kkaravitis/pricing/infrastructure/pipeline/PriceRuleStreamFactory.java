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

import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.PriceRuleUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource.KafkaSourceContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PriceRuleStreamFactory {

    public DataStream<MetricUpdate> build(StreamExecutionEnvironment env, Configuration config) {

        CommonKafkaSource<PriceRuleUpdate> priceRuleSource = new CommonKafkaSource<>(KafkaSourceContext
              .<PriceRuleUpdate>builder()
              .topic(config.get(PricingConfigOptions.KAFKA_PRICERULE_TOPIC))
              .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
              .groupId(config.get(PricingConfigOptions.KAFKA_PRICERULE_GROUP_ID))
              .sourceId("PriceRuleUpdate")
              .messageType(PriceRuleUpdate.class)
              .bounded(config.get(PricingConfigOptions.TEST_MODE))
              .watermarkStrategySupplier(WatermarkStrategy::forMonotonousTimestamps)
              .build());


        DataStream<PriceRuleUpdate> ruleUpdates = priceRuleSource.create(env);

        return ruleUpdates
              .map(rule -> new MetricUpdate(rule.productId(), MetricType.RULE, rule))
              .name("PriceRuleUpdate");
    }

}
