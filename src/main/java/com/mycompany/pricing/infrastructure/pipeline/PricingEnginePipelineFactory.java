package com.mycompany.pricing.infrastructure.pipeline;

import com.mycompany.pricing.adapters.FlinkDemandMetricsRepository;
import com.mycompany.pricing.adapters.FlinkInventoryLevelRepository;
import com.mycompany.pricing.adapters.FlinkPriceRuleRepository;
import com.mycompany.pricing.adapters.competitor.FlinkCompetitorPriceRepository;
import com.mycompany.pricing.adapters.ml.MlModelAdapter;
import com.mycompany.pricing.domain.ClickEvent;
import com.mycompany.pricing.infrastructure.source.KafkaModelBroadcastSource;
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
