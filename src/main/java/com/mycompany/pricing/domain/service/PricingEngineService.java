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
    public PricingResult computePrice(PricingContext ctx, Money mlBasePrice) {
        // 1) Start from ML base price
        Money price = mlBasePrice;

        // 2) Blend in competitor price (e.g. weighted average)
        Money comp = ctx.getCompetitorPrice().getPrice();  // assume Money
        price = price.multiply(0.7)
              .add(comp.multiply(0.3));

        // Adjust for demand: if currentDemand > historicalAverage, increase price by 5%
        DemandMetrics dm = ctx.getDemandMetrics();
        if (dm.getCurrentDemand() > dm.getHistoricalAverage()) {
            price = price.multiply(1.05);
        }

        // 3) Adjust for low inventory (e.g. +10% if < threshold)
        if (ctx.getInventoryLevel() < 10) {
            price = price.multiply(1.1);
        }

        // 4) Clamp within the PriceRule
        PriceRule rule = ctx.getPriceRule();
        if (price.isLessThan(rule.getMinPrice())) {
            price = rule.getMinPrice();
        } else if (price.isGreaterThan(rule.getMaxPrice())) {
            price = rule.getMaxPrice();
        }

        // 5) Build result
        return new PricingResult(
              ctx.getProduct(),
              price
        );
    }
}
