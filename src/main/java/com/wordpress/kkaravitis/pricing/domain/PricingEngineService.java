/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

import java.time.Instant;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * The core application service orchestrating the pricing algorithm:
 * 1) Retrieves all contextual inputs via ports.
 * 2) Invokes the ML inference port.
 * 3) Blends and adjusts prices based on competitor data and inventory.
 * 4) Enforces business min/max price rules.
 */
@Slf4j
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
        DemandMetrics demandMetrics   = demandMetricsRepository.getDemandMetrics(product.productId());
        int inventoryLevel = inventoryLevelRepository.getInventoryLevel(product.productId());
        CompetitorPrice cp = competitorPriceRepository.getCompetitorPrice(product.productId());
        PriceRule rule     = priceRuleRepository.getPriceRule(product.productId());

        log.info("[PRICING] Product id = {}", product.productId());
        log.info("[PRICING] Product name = {}", product.productName());
        log.info("[PRICING] demand = {}", demandMetrics.currentDemand());
        log.info("[PRICING] inventory = {}", inventoryLevel);
        log.info("[PRICING] competitor price = {}", cp);

        // fallback if no rule yet
        if (rule == null) {
            rule = PriceRule.defaults();
        }

        // 2) Build a context for ML inference
        PricingContext ctx = new PricingContext(
              product, demandMetrics, inventoryLevel, cp, rule
        );

        // 3) ML base price
        Money mlPrice = modelInferencePricePredictor.predictPrice(ctx);
        log.info("[PRICING] ML prediction = {}", mlPrice);

        // 4) Blend competitor (70/30)
        Money price = mlPrice.multiply(0.7)
              .add(cp.price().multiply(0.3));

        // 5) Demand adjustment
        if (demandMetrics.currentDemand() > demandMetrics.historicalAverage()) {
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
        return new PricingResult(
              product.productId(),
              product.productName(),
              price,
              Instant.now().toEpochMilli());
    }
}
