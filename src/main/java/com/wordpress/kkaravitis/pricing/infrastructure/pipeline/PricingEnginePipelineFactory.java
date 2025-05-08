package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkDemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkInventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.adapters.ml.MlModelAdapter;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;
import com.wordpress.kkaravitis.pricing.infrastructure.source.KafkaModelBroadcastSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PricingEnginePipelineFactory {
        public void build(DataStream<ClickEvent> clicks, StreamExecutionEnvironment env) {
            KafkaModelBroadcastSource modelCdc =
                  new KafkaModelBroadcastSource("localhost:9092", "model-topic", "model-group");//TODO: Pass from configuration file
            clicks
                  .keyBy(ClickEvent::getProductId)
                  .connect(modelCdc.create(env))
                  .process(new PricingWithModelBroadcastFunction(
                        new FlinkPriceRuleRepository(),
                        new FlinkDemandMetricsRepository(),
                        new FlinkInventoryLevelRepository(),
                        new FlinkCompetitorPriceRepository(),
                        new MlModelAdapter("current-model")
                  ))
                  .name("DynamicPricingUnified")
                  .print();

        }
}
