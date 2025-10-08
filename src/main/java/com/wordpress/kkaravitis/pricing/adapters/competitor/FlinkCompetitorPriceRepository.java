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
package com.wordpress.kkaravitis.pricing.adapters.competitor;

import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPriceRepository;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * Keyed ValueState storing latest competitor price per product.
 */
public class FlinkCompetitorPriceRepository implements CompetitorPriceRepository, Serializable {

    private transient ValueState<CompetitorPrice> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<CompetitorPrice> desc =
              new ValueStateDescriptor<>(
                    "competitor-price",
                    Types.POJO(CompetitorPrice.class)
              );
        this.state = ctx.getState(desc);
    }

    /**
     * Called from your async enrichment ProcessFunction.
     */
    public void updatePrice(CompetitorPrice price) throws PricingException {
        try {
            state.update(price);
        } catch (IOException exception) {
            throw new PricingException("Could not update Flink state with competitor price", exception);
        }
    }

    @Override
    public CompetitorPrice getCompetitorPrice(String productId) throws PricingException {
        try{
            CompetitorPrice cp = state.value();
            return cp != null ? cp : new CompetitorPrice(productId, "", new Money(0.0, "EUR"));
        } catch (IOException exception) {
            throw new PricingException("Could not fetch competitor price from flink state", exception);
        }

    }
}
