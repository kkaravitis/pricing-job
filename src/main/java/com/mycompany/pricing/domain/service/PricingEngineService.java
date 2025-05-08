package com.mycompany.pricing.domain.service;

import com.mycompany.pricing.domain.model.*;
import com.mycompany.pricing.domain.port.*;
import java.math.BigDecimal;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The core application service orchestrating the pricing algorithm:
 * 1) Retrieves all contextual inputs via ports.
 * 2) Invokes the ML inference port.
 * 3) Blends and adjusts prices based on competitor data and inventory.
 * 4) Enforces business min/max price rules.
 */
@Getter
@RequiredArgsConstructor
public class PricingEngineService {
    private final DemandMetricsProvider demandMetricsProvider;
    private final InventoryProvider inventoryProvider;
    private final CompetitorPriceProvider competitorPriceProvider;
    private final PriceRuleProvider ruleProvider;
    private final ModelInferencePort modelInferencePort;

    /**
     * Calculates the final price for a given product ID.
     * @return a PricingResult containing the new price in Money
     */
    public PricingResult computePrice(String productId) throws Exception {
        // 1) Gather data from ports
        Product product    = new Product(productId);
        DemandMetrics dm   = demandMetricsProvider.getDemandMetrics(productId);
        int inventoryLevel = inventoryProvider.getInventoryLevel(productId);
        CompetitorPrice cp = competitorPriceProvider.getCompetitorPrice(productId);
        PriceRule rule     = ruleProvider.getPriceRule(productId);

        // fallback if no rule yet
        if (rule == null) {
            rule = PriceRule.defaults();
        }

        // 2) Build a context for ML inference
        PricingContext ctx = new PricingContext(
              product, dm, inventoryLevel, cp, rule
        );

        // 3) ML base price
        Money mlPrice = modelInferencePort.predictPrice(ctx);

        // 4) Blend competitor (70/30)
        Money price = mlPrice.multiply(0.7)
              .add(cp.getPrice().multiply(0.3));

        // 5) Demand adjustment
        if (dm.getCurrentDemand() > dm.getHistoricalAverage()) {
            price = price.multiply(1.05);
        }

        // 6) Inventory adjustment
        if (inventoryLevel < 10) {
            price = price.multiply(1.1);
        }

        // 7) Clamp within rules
        if (price.isLessThan(rule.getMinPrice())) {
            price = rule.getMinPrice();
        } else if (price.isGreaterThan(rule.getMaxPrice())) {
            price = rule.getMaxPrice();
        }

        // 8) Return result
        return new PricingResult(product, price);
    }
}
