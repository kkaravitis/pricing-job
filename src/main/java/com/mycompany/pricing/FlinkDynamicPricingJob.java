package com.mycompany.pricing;

import com.mycompany.pricing.domain.ClickEvent;
import com.mycompany.pricing.infrastructure.pipeline.CompetitorPricePipelineFactory;
import com.mycompany.pricing.infrastructure.pipeline.DemandMetricsPipelineFactory;
import com.mycompany.pricing.infrastructure.pipeline.InventoryPipelineFactory;
import com.mycompany.pricing.infrastructure.pipeline.PriceRulePipelineFactory;
import com.mycompany.pricing.infrastructure.pipeline.PricingEnginePipelineFactory;
import com.mycompany.pricing.infrastructure.source.KafkaClickEventSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink job that unifies price-rule and model updates via a single broadcast stream, enriches with async competitor prices, and computes dynamic pricing.
 */
public class FlinkDynamicPricingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        // Click events
        KafkaClickEventSource clicksSource =
              new KafkaClickEventSource("localhost:9092", "click-topic", "click-group");
        DataStream<ClickEvent> clicks = clicksSource.create(env);

        CompetitorPricePipelineFactory competitorPricePipelineFactory = new CompetitorPricePipelineFactory();
        competitorPricePipelineFactory.build(clicks);

        DemandMetricsPipelineFactory demandMetricsPipelineFactory = new DemandMetricsPipelineFactory();
        demandMetricsPipelineFactory.build(clicks);

        InventoryPipelineFactory inventoryPipelineFactory = new InventoryPipelineFactory();
        inventoryPipelineFactory.build(env);

        PriceRulePipelineFactory priceRulePipelineFactory = new PriceRulePipelineFactory();
        priceRulePipelineFactory.build(env);

        PricingEnginePipelineFactory pricingEnginePipelineFactory = new PricingEnginePipelineFactory();
        pricingEnginePipelineFactory.build(clicks, env);

        env.execute("Flink Dynamic Pricing Job");
    }
}
