/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkEmergencyAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.ConfigurationFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.CompetitorPriceStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.DemandMetricsStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.EmergencyPriceAdjustmentsStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.InventoryStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.stream.PriceRuleStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.PricingEnginePipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.CommonKafkaSource.KafkaSourceContext;
import lombok.Builder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * Flink job that unifies price-rule and model updates via a single broadcast stream, enriches with async competitor prices, and computes dynamic pricing.
 */
@Builder
public class FlinkDynamicPricingJob {

    private final FlinkDemandMetricsRepository flinkDemandMetricsRepository;
    private final FlinkCompetitorPriceRepository flinkCompetitorPriceRepository;
    private final FlinkInventoryLevelRepository flinkInventoryLevelRepository;
    private final FlinkPriceRuleRepository flinkPriceRuleRepository;
    private final FlinkEmergencyAdjustmentRepository flinkEmergencyAdjustmentRepository;
    private final MlModelAdapter mlModelAdapter;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration config = new ConfigurationFactory().build(params);

        FlinkDynamicPricingJob.builder()
              .flinkCompetitorPriceRepository(new FlinkCompetitorPriceRepository())
              .flinkDemandMetricsRepository(new FlinkDemandMetricsRepository())
              .flinkInventoryLevelRepository(new FlinkInventoryLevelRepository())
              .flinkPriceRuleRepository(new FlinkPriceRuleRepository())
              .flinkEmergencyAdjustmentRepository(new FlinkEmergencyAdjustmentRepository())
              .mlModelAdapter(new MlModelAdapter())
              .build()
              .execute(env, config);
    }

    public void execute(StreamExecutionEnvironment env, Configuration config) throws Exception {
        // Click events
        DataStream<ClickEvent> clicks =
              new CommonKafkaSource<>(KafkaSourceContext.<ClickEvent>builder()
                    .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .groupId(config.get(PricingConfigOptions.KAFKA_CLICK_GROUP_ID))
                    .topic(config.get(PricingConfigOptions.KAFKA_CLICK_TOPIC))
                    .sourceId("clickEvents")
                    .messageType(ClickEvent.class)
                    .bounded(config.get(PricingConfigOptions.TEST_MODE))
                    .watermarkStrategySupplier(WatermarkStrategy::noWatermarks)
                    .build())
                    .create(env);

        DataStream<MetricUpdate> demandStream = new DemandMetricsStreamFactory().build(clicks);
        DataStream<MetricUpdate> competitorStream =  new CompetitorPriceStreamFactory().build(clicks, config);
        DataStream<MetricUpdate> inventoryStream = new InventoryStreamFactory().build(env, config);
        DataStream<MetricUpdate> priceRuleStream = new PriceRuleStreamFactory().build(env, config);
        DataStream<MetricUpdate> emergencyStream = new EmergencyPriceAdjustmentsStreamFactory().build(env, config);

        DataStream<MetricUpdate> metricsUnion =
              demandStream.union(competitorStream, inventoryStream, priceRuleStream, emergencyStream);

        new PricingEnginePipelineFactory().build(clicks, env, config, metricsUnion);

        env.execute("Flink Dynamic Pricing Job");
    }
}
