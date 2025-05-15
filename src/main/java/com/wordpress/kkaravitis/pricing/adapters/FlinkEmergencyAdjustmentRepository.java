package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.EmergencyPriceAdjustmentRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

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
              .newBuilder(Time.minutes(10))
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
