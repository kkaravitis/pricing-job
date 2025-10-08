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
    private final EmergencyPriceAdjustmentRepository emergencyPriceAdjustmentRepository;

    /**
     * Calculates the final price for a given product ID.
     * @return a PricingResult containing the new price in Money
     */
    public PricingResult computePrice(Product product) throws PricingException {
        // 1) Gather data from ports
        DemandMetrics dm   = demandMetricsRepository.getDemandMetrics(product.productId());
        int inventoryLevel = inventoryLevelRepository.getInventoryLevel(product.productId());
        CompetitorPrice cp = competitorPriceRepository.getCompetitorPrice(product.productId());
        PriceRule rule     = priceRuleRepository.getPriceRule(product.productId());

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
              .add(cp.price().multiply(0.3));

        // 5) Demand adjustment
        if (dm.currentDemand() > dm.historicalAverage()) {
            price = price.multiply(1.05);
        }

        // 6) Inventory adjustment
        if (inventoryLevel < 10) {
            price = price.multiply(1.1);
        }

        // 7) Emergency spike adjustment (e.g. flash-sale factor)
        double emergFactor = emergencyPriceAdjustmentRepository.getAdjustmentFactor(product.productId());
        if (emergFactor > 1.0) {
            price = price.multiply(emergFactor);
        }

        // 8) Clamp within rules
        if (price.isLessThan(rule.minPrice())) {
            price = rule.minPrice();
        } else if (price.isGreaterThan(rule.maxPrice())) {
            price = rule.maxPrice();
        }

        // 9) Return result
        return new PricingResult(product.productId(), product.productName(), price);
    }
}
