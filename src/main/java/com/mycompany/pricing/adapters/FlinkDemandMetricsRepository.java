// src/main/java/com/mycompany/pricing/infrastructure/provider/FlinkDemandMetricsProvider.java
package com.mycompany.pricing.adapters;

import com.mycompany.pricing.domain.DemandMetrics;
import com.mycompany.pricing.domain.DemandMetricsRepository;
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
    public void updateMetrics(String productId, DemandMetrics metrics) throws Exception {
        state.put(productId, metrics);
    }

    @Override
    public DemandMetrics getDemandMetrics(String productId) throws Exception {
        DemandMetrics dm = state.get(productId);
        return dm != null ? dm : new DemandMetrics(productId,0.0, 0.0);
    }
}
