package com.wordpress.kkaravitis.pricing.domain;

/**
 * Wraps a competitor's current price for a given product. Used to perform competitive benchmarking in the pricing algorithm.
 */
public record CompetitorPrice(String productId, Money price) {

}