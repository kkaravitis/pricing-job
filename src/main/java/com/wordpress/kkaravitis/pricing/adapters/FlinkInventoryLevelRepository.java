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
              new ValueStateDescriptor<>("inventory-level", Types.INT);
        this.state = ctx.getState(desc);
    }

    public void updateLevel(int level) throws PricingException {
        try {
            state.update(level);
        } catch (IOException e) {
            throw new PricingException("Failed to update the inventory level flink state.", e);
        }
    }

    @Override
    public int getInventoryLevel(String productId) throws PricingException {
        Integer value;
        try {
            value = state.value();
        } catch (IOException e) {
            throw new PricingException("Failed to fetch inventory level flink state.", e);
        }
        return value == null ? 0 : value;
    }
}
