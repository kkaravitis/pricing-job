package com.mycompany.pricing.infrastructure.provider;

import com.mycompany.pricing.domain.model.PriceRule;
import com.mycompany.pricing.domain.port.PriceRuleProvider;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Keyed state store for per‐product PriceRule.
 */
public class PriceRuleProviderAdapter implements PriceRuleProvider, Serializable {
    private transient ValueState<PriceRule> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<PriceRule> desc =
              new ValueStateDescriptor<>("price-rule", PriceRule.class);
        state = ctx.getState(desc);
    }

    /** Called from rule‐feeder to update the rule for the current key. */
    public void updateRule(PriceRule rule) throws Exception {
        state.update(rule);
    }

    @Override
    public PriceRule getPriceRule(String productId) throws Exception {
        return state.value();
    }
}
