package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.adapters.FlinkPriceRuleRepository;
import com.wordpress.kkaravitis.pricing.domain.PriceRuleUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.PriceRuleCdcSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.PriceRuleCdcSource.PriceRuleCdcSourceContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PriceRulePipelineFactory {

    public void build(StreamExecutionEnvironment env, Configuration config) {
        PriceRuleCdcSource ruleCdc = new PriceRuleCdcSource(
              PriceRuleCdcSourceContext
                    .builder()
                    .hostname(config.get(PricingConfigOptions.PRICERULE_CDC_HOST))
                    .database(config.get(PricingConfigOptions.PRICERULE_CDC_DATABASE))
                    .port(config.get(PricingConfigOptions.PRICERULE_CDC_PORT))
                    .table(config.get(PricingConfigOptions.PRICERULE_CDC_TABLE))
                    .username(config.get(PricingConfigOptions.PRICERULE_CDC_USER))
                    .password(config.get(PricingConfigOptions.PRICERULE_CDC_PASSWORD))
                    .build());
        DataStream<PriceRuleUpdate> ruleUpdates = ruleCdc.create(env);

        FlinkPriceRuleRepository ruleProv = new FlinkPriceRuleRepository();

        ruleUpdates
              .keyBy(PriceRuleUpdate::productId)
              .process(new KeyedProcessFunction<String, PriceRuleUpdate, Void>() {
                  @Override
                  public void open(OpenContext openContext) {
                      ruleProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(PriceRuleUpdate upd, Context ctx, Collector<Void> out)
                        throws Exception {
                      ruleProv.updateRule(upd.priceRule());
                  }
              })
              .name("UpdatePriceRuleState");
    }

}
