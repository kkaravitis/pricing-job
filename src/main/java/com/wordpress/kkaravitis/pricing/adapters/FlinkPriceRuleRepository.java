/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

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
 * PriceRuleRepository Flink adapter
 */
public class FlinkPriceRuleRepository implements PriceRuleRepository, Serializable {
    private transient ValueState<PriceRule> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<PriceRule> desc =
              new ValueStateDescriptor<>("price-rule", PriceRule.class);
        state = ctx.getState(desc);
    }

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
