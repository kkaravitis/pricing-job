package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.EmergencyPriceAdjustmentRepository;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import org.apache.flink.api.common.time.Time;

/**
 * ValueState holding the latest emergency multiplier (default 1.0).
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
    public void updateAdjustment(String productId, double factor) throws Exception {
        state.update(factor);
    }

    @Override
    public double getAdjustmentFactor(String productId) throws Exception {
        Double f = state.value();
        // manually fallback to 1.0 if no value yet
        return (f != null ? f : 1.0);
    }
}
