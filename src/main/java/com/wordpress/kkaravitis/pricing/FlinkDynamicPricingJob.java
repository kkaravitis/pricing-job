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
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.EmergencyPriceAdjustmentsStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.CompetitorPriceStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.DemandMetricsStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.InventoryStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.PriceRuleStreamFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.PricingEnginePipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaClickEventSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaClickEventSource.KafkaClickEventSourceContext;
import lombok.Builder;
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
        KafkaClickEventSource clicksSource =
              new KafkaClickEventSource(KafkaClickEventSourceContext.builder()
                    .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .topic(config.get(PricingConfigOptions.KAFKA_CLICK_TOPIC))
                    .groupId(config.get(PricingConfigOptions.KAFKA_CLICK_GROUP_ID))
                    .build());

        DataStream<ClickEvent> clicks = clicksSource.create(env);

        DataStream<MetricUpdate> competitorStream =  new CompetitorPriceStreamFactory().build(clicks, config);
        DataStream<MetricUpdate> demandStream = new DemandMetricsStreamFactory().build(clicks);
        DataStream<MetricUpdate> inventoryStream = new InventoryStreamFactory().build(env, config);
        DataStream<MetricUpdate> priceRuleStream = new PriceRuleStreamFactory().build(env, config);
        DataStream<MetricUpdate> emergencyStream = new EmergencyPriceAdjustmentsStreamFactory().build(env, config);

        DataStream<MetricUpdate> metricsUnion =
              demandStream.union(competitorStream, inventoryStream, priceRuleStream, emergencyStream);

        new PricingEnginePipelineFactory().build(clicks, env, config, metricsUnion);

        env.execute("Flink Dynamic Pricing Job");
    }
}
