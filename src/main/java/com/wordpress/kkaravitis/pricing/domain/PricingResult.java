package com.wordpress.kkaravitis.pricing.domain;

/**
 * Encapsulates the outcome of the pricing computation.
 * - product: the target product for which the price was calculated.
 * - newPrice: the final price to be
 * published.
 */
public record PricingResult(Product product, Money newPrice) {

}
