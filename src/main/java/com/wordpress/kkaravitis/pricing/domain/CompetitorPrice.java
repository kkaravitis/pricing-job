package com.wordpress.kkaravitis.pricing.domain;

import lombok.Value;

/**
 * Wraps a competitor's current price for a given product.
 * Used to perform competitive benchmarking in the pricing algorithm.
 */
@Value
public class CompetitorPrice {
    String productId;
    Money price;
}