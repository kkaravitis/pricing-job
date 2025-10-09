/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream;

import com.wordpress.kkaravitis.pricing.domain.InventoryEvent;
import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource.KafkaSourceContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class InventoryStreamFactory {
    public DataStream<MetricUpdate> build(StreamExecutionEnvironment env, Configuration config) {

        CommonKafkaSource<InventoryEvent> inventorySource =
              new CommonKafkaSource<>(KafkaSourceContext.<InventoryEvent>builder()
                    .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .topic(config.get(PricingConfigOptions.KAFKA_INVENTORY_TOPIC))
                    .groupId(config.get(PricingConfigOptions.KAFKA_INVENTORY_GROUP_ID))
                    .bounded(config.get(PricingConfigOptions.TEST_MODE))
                    .watermarkStrategySupplier(WatermarkStrategy::forMonotonousTimestamps)
                    .messageType(InventoryEvent.class)
                    .sourceId("Inventory")
                    .build());

        DataStream<InventoryEvent> inventoryStream = inventorySource.create(env);

        return inventoryStream
              .map(i -> new MetricUpdate(i.productId(), MetricType.INVENTORY, i))
              .name("InventoryUpdate");
    }
}
