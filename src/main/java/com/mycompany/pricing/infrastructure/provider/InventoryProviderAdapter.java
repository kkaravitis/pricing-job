// src/main/java/com/mycompany/pricing/infrastructure/provider/FlinkInventoryProvider.java
package com.mycompany.pricing.infrastructure.provider;

import com.mycompany.pricing.domain.port.InventoryProvider;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/** Keyed ValueState for inventory levels. */
public class InventoryProviderAdapter implements InventoryProvider, Serializable {
    private transient ValueState<Integer> state;

    public void initializeState(RuntimeContext ctx) {
        ValueStateDescriptor<Integer> desc =
              new ValueStateDescriptor<>("inventory-level", Types.INT);
        this.state = ctx.getState(desc);
    }

    /** Called by your inventory‐event ProcessFunction. */
    public void updateLevel(int level) throws Exception {
        state.update(level);
    }

    @Override
    public int getInventoryLevel(String productId) throws Exception {
        Integer v = state.value();
        return v == null ? 0 : v;
    }
}
