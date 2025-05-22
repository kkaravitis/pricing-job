package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Encapsulates a dynamic update to pricing rules for a single product. Emitted via CDC or Kafka and applied via broadcast state.
 */
public record PriceRuleUpdate(String productId, PriceRule priceRule) implements Serializable {

}