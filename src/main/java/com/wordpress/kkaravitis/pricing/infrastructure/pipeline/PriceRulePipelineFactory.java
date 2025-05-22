package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.wordpress.kkaravitis.pricing.domain.MetricType;
import com.wordpress.kkaravitis.pricing.domain.MetricUpdate;
import com.wordpress.kkaravitis.pricing.domain.PriceRuleUpdate;
import com.wordpress.kkaravitis.pricing.infrastructure.config.PricingConfigOptions;
import com.wordpress.kkaravitis.pricing.infrastructure.source.PriceRuleCdcSource;
import com.wordpress.kkaravitis.pricing.infrastructure.source.PriceRuleCdcSource.PriceRuleCdcSourceContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PriceRulePipelineFactory {

    public DataStream<MetricUpdate> build(StreamExecutionEnvironment env, Configuration config) {
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

        return ruleUpdates
              .map(rule -> new MetricUpdate(rule.productId(), MetricType.RULE, rule))
              .name("PriceRuleUpdate");
    }

}
