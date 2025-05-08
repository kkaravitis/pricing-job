package com.mycompany.pricing.domain.model;

import lombok.Value;

/**
 * Encapsulates a dynamic update to pricing rules for a single product.
 * Emitted via CDC or Kafka and applied via broadcast state.
 */
@Value
public class PriceRuleUpdate {
    String productId;
    PriceRule priceRule;
}