package com.wordpress.kkaravitis.pricing;

import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * Flink job that unifies price-rule and model updates via a single broadcast stream, enriches with async competitor prices, and computes dynamic pricing.
 */
public class FlinkDynamicPricingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration config = new ConfigurationFactory().build(params);

        // Click events
        KafkaClickEventSource clicksSource =
              new KafkaClickEventSource(KafkaClickEventSourceContext.builder()
                    .brokers(config.get(PricingConfigOptions.KAFKA_BOOTSTRAP_SERVERS))
                    .topic(config.get(PricingConfigOptions.KAFKA_CLICK_TOPIC))
                    .groupId(config.get(PricingConfigOptions.KAFKA_CLICK_GROUP_ID))
                    .build());

        DataStream<ClickEvent> clicks = clicksSource.create(env);

        new CompetitorPricePipelineFactory().build(clicks, config);
        new DemandMetricsPipelineFactory().build(clicks);
        new InventoryPipelineFactory().build(env, config);
        new PriceRulePipelineFactory().build(env, config);
        new AnomalyDetectionPipelineFactory().build(env, config);
        new PricingEnginePipelineFactory().build(clicks, env, config);

        env.execute("Flink Dynamic Pricing Job");
    }
}
