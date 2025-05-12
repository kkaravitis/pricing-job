package com.wordpress.kkaravitis.pricing.adapters;

import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.DemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/** Keyed MapState for rolling demand metrics. */
public class FlinkDemandMetricsRepository implements DemandMetricsRepository, Serializable {
    private transient MapState<String, DemandMetrics> state;

    public void initializeState(RuntimeContext ctx) {
        MapStateDescriptor<String, DemandMetrics> desc =
              new MapStateDescriptor<>(
                    "demand-metrics",
                    Types.STRING,
                    Types.POJO(DemandMetrics.class)
              );
        this.state = ctx.getMapState(desc);
    }

    /** Called after your sliding‐window onClick ProcessFunction. */
    public void updateMetrics(String productId, DemandMetrics metrics) throws PricingException {
        try {
            state.put(productId, metrics);
        } catch (Exception e) {
            throw new PricingException("Failed to update demand metrics flink state.", e);
        }
    }

    @Override
    public DemandMetrics getDemandMetrics(String productId) throws PricingException {
        DemandMetrics dm = null;
        try {
            dm = state.get(productId);
        } catch (Exception e) {
            throw new PricingException("Failed to fetch demand metrics from flink state.", e);
        }
        return dm != null ? dm : new DemandMetrics(productId,0.0, 0.0);
    }
}
