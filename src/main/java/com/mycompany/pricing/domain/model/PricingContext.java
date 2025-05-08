package com.mycompany.pricing.domain.model;

import lombok.Value;

/**
 * Aggregates all contextual data needed by the pricing algorithm.
 * Combines product details, demand metrics, inventory levels,
 * competitor pricing, and business rules into one cohesive object.
 */
@Value
public class PricingContext {
    Product product;
    DemandMetrics demandMetrics;
    int inventoryLevel;
    CompetitorPrice competitorPrice;
    PriceRule priceRule;
}