/*
 * Copyright 2025 Konstantinos Karavitis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
