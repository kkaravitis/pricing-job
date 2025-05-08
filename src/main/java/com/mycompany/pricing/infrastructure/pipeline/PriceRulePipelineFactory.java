package com.mycompany.pricing.infrastructure.pipeline;

import com.mycompany.pricing.adapters.FlinkPriceRuleRepository;
import com.mycompany.pricing.domain.PriceRuleUpdate;
import com.mycompany.pricing.infrastructure.source.PriceRuleCdcSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PriceRulePipelineFactory {

    public void build(StreamExecutionEnvironment env) {
        PriceRuleCdcSource ruleCdc = new PriceRuleCdcSource(
              "db-host", 3306, "pricing_db", "price_rules", "dbuser", "dbpass");
        DataStream<PriceRuleUpdate> ruleUpdates = ruleCdc.create(env);

        FlinkPriceRuleRepository ruleProv = new FlinkPriceRuleRepository();

        ruleUpdates
              .keyBy(PriceRuleUpdate::getProductId)
              .process(new KeyedProcessFunction<String, PriceRuleUpdate, Void>() {
                  @Override
                  public void open(Configuration cfg) {
                      ruleProv.initializeState(getRuntimeContext());
                  }

                  @Override
                  public void processElement(PriceRuleUpdate upd, Context ctx, Collector<Void> out)
                        throws Exception {
                      ruleProv.updateRule(upd.getPriceRule());
                  }
              })
              .name("UpdatePriceRuleState");
    }

}
