/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
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
