package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Wraps a competitor's current price for a given product. Used to perform competitive benchmarking in the pricing algorithm.
 */
public record CompetitorPrice(String productId, Money price) implements Serializable {

}