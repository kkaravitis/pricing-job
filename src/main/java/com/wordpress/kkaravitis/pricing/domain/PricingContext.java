/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

/**
 * Aggregates all contextual data needed by the pricing algorithm. Combines product details, demand metrics, inventory levels, competitor pricing, and business
 * rules into one cohesive object.
 */
public record PricingContext(Product product, DemandMetrics demandMetrics, int inventoryLevel, CompetitorPrice competitorPrice, PriceRule priceRule) {

}