/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.InventoryLevelRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * InventoryLevelRepository Flink adapter.
 * */
public class FlinkInventoryLevelRepository implements InventoryLevelRepository, Serializable {
    private transient ValueState<Integer> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<Integer> desc =
              new ValueStateDescriptor<>("inventory-quantity", Types.INT);
        this.state = ctx.getState(desc);
    }

    public void updateLevel(int level) throws PricingException {
        try {
            state.update(level);
        } catch (IOException e) {
            throw new PricingException("Failed to update the inventory quantity flink state.", e);
        }
    }

    @Override
    public int getInventoryLevel(String productId) throws PricingException {
        Integer value;
        try {
            value = state.value();
        } catch (IOException e) {
            throw new PricingException("Failed to fetch inventory quantity flink state.", e);
        }
        return value == null ? 0 : value;
    }
}
