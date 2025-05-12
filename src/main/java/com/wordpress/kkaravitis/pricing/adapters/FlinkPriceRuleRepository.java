package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.PriceRule;
import com.wordpress.kkaravitis.pricing.domain.PriceRuleRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Keyed state store for per‐product PriceRule.
 */
public class FlinkPriceRuleRepository implements PriceRuleRepository, Serializable {
    private transient ValueState<PriceRule> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<PriceRule> desc =
              new ValueStateDescriptor<>("price-rule", PriceRule.class);
        state = ctx.getState(desc);
    }

    /** Called from rule‐feeder to update the rule for the current key. */
    public void updateRule(PriceRule rule) throws PricingException {
        try {
            state.update(rule);
        } catch (IOException e) {
            throw new PricingException("Failed to update price rule in flink state.", e);
        }
    }

    @Override
    public PriceRule getPriceRule(String productId) throws PricingException {
        try {
            return state.value();
        } catch (IOException e) {
            throw new PricingException("Failed to fetch price rule from flink state.", e);
        }
    }
}
