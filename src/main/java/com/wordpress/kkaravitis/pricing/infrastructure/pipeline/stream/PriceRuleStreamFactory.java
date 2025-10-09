/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

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
