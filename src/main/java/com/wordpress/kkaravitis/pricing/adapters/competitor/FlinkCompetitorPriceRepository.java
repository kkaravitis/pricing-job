package com.wordpress.kkaravitis.pricing.adapters.competitor;

import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPriceRepository;
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
    public void updatePrice(CompetitorPrice price) throws Exception {
        state.update(price);
    }

    @Override
    public CompetitorPrice getCompetitorPrice(String productId) throws Exception {//TODO: Replace with application dedicated exception
        CompetitorPrice cp = state.value();
        return cp != null ? cp : new CompetitorPrice(productId, new Money(0.0, "USD"));
    }
}
