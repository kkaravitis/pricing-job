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
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.AnomalyDetectionPipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.CompetitorPricePipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.DemandMetricsPipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.InventoryPipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.PriceRulePipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.pipeline.PricingEnginePipelineFactory;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaClickEventSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaClickEventSource.KafkaClickEventSourceContext;
import lombok.Builder;
import lombok.Data;
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

        DataStream<MetricUpdate> competitorStream =  new CompetitorPricePipelineFactory().build(clicks, config);
        DataStream<MetricUpdate> demandStream = new DemandMetricsPipelineFactory().build(clicks);
        DataStream<MetricUpdate> inventoryStream = new InventoryPipelineFactory().build(env, config);
        DataStream<MetricUpdate> priceRuleStream = new PriceRulePipelineFactory().build(env, config);
        DataStream<MetricUpdate> emergencyStream = new AnomalyDetectionPipelineFactory().build(env, config);

        DataStream<MetricUpdate> metricsUnion =
              demandStream.union(competitorStream, inventoryStream, priceRuleStream, emergencyStream);

        new PricingEnginePipelineFactory().build(clicks, env, config, metricsUnion);

        env.execute("Flink Dynamic Pricing Job");
    }
}
