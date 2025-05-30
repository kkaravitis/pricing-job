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

import com.wordpress.kkaravitis.pricing.domain.EmergencyPriceAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * EmergencyPriceAdjustmentRepository flink adapter.
 */
public class FlinkEmergencyAdjustmentRepository
      implements EmergencyPriceAdjustmentRepository, Serializable {

    private transient ValueState<Double> state;

    /**
     * Initialize keyed state descriptor *without* a built-in default.
     */
    public void initializeState(RuntimeContext ctx) {

        StateTtlConfig ttlConfig = StateTtlConfig
              .newBuilder(Duration.ofMinutes(10))
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .build();

        ValueStateDescriptor<Double> valueStateDescriptor =
              // no default value provided here:
              new ValueStateDescriptor<>("emergency-adjustment", Double.class);
        this.state = ctx.getState(valueStateDescriptor);

        valueStateDescriptor.enableTimeToLive(ttlConfig);

        this.state = ctx.getState(valueStateDescriptor);
    }

    /**
     * Called by the CEP‐pipeline to update the factor (e.g. 1.2).
     */
    public void updateAdjustment(double factor) throws PricingException {
        try {
            state.update(factor);
        } catch (IOException e) {
            throw new PricingException("Failed to update emergency adjustment factor in flink state.", e);
        }
    }

    @Override
    public double getAdjustmentFactor(String productId) throws PricingException {
        Double factor;
        try {
            factor = state.value();
        } catch (IOException exception) {
            throw new PricingException("Failed to fetch the emergency adjustment factor from flink state.", exception);
        }
        // manually fallback to 1.0 if no value yet
        return (factor != null ? factor : 1.0);
    }
}
