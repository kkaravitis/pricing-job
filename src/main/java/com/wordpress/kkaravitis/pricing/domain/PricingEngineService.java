package com.wordpress.kkaravitis.pricing.domain;

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
    private final DemandMetricsRepository demandMetricsRepository;
    private final InventoryLevelRepository inventoryLevelRepository;
    private final CompetitorPriceRepository competitorPriceRepository;
    private final PriceRuleRepository priceRuleRepository;
    private final ModelInferencePricePredictor modelInferencePricePredictor;

    /**
     * Calculates the final price for a given product ID.
     * @return a PricingResult containing the new price in Money
     */
    public PricingResult computePrice(String productId) throws Exception {//TODO: Replace with application dedicated exception
        // 1) Gather data from ports
        Product product    = new Product(productId);
        DemandMetrics dm   = demandMetricsRepository.getDemandMetrics(productId);
        int inventoryLevel = inventoryLevelRepository.getInventoryLevel(productId);
        CompetitorPrice cp = competitorPriceRepository.getCompetitorPrice(productId);
        PriceRule rule     = priceRuleRepository.getPriceRule(productId);

        // fallback if no rule yet
        if (rule == null) {
            rule = PriceRule.defaults();
        }

        // 2) Build a context for ML inference
        PricingContext ctx = new PricingContext(
              product, dm, inventoryLevel, cp, rule
        );

        // 3) ML base price
        Money mlPrice = modelInferencePricePredictor.predictPrice(ctx);

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
