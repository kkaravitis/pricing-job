package com.wordpress.kkaravitis.pricing.domain;

/**
 * Aggregates all contextual data needed by the pricing algorithm. Combines product details, demand metrics, inventory levels, competitor pricing, and business
 * rules into one cohesive object.
 */
public record PricingContext(Product product, DemandMetrics demandMetrics, int inventoryLevel, CompetitorPrice competitorPrice, PriceRule priceRule) {

}