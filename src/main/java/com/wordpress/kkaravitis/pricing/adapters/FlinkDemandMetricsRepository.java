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

import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.DemandMetricsRepository;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * DemandMetricsRepository Flink adapter.
 **/
public class FlinkDemandMetricsRepository implements DemandMetricsRepository, Serializable {
    private transient ValueState<DemandMetrics> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<DemandMetrics> desc =
              new ValueStateDescriptor<>(
                    "demand-metrics",
                    Types.POJO(DemandMetrics.class)
              );
        this.state = ctx.getState(desc);
    }

    /**
     * Updates metrics.
     **/
    public void updateMetrics(DemandMetrics metrics) throws PricingException {
        try {
            state.update(metrics);
        } catch (Exception exception) {
            throw new PricingException(String.format(
                  "Failed to update demand metrics flink state for product id: %s.", metrics.productId()
            ), exception);
        }
    }

    @Override
    public DemandMetrics getDemandMetrics(String productId) throws PricingException {
        DemandMetrics demandMetrics;
        try {
            demandMetrics = state.value();
        } catch (Exception exception) {
            throw new PricingException(String
                  .format("Failed to fetch demand metrics from flink state for product id: %s.", productId),
                  exception);
        }
        return demandMetrics != null ? demandMetrics :
              new DemandMetrics(productId,"",0.0, 0.0);
    }
}
