package com.mycompany.pricing.domain;

import lombok.Value;

/**
 * Encapsulates the outcome of the pricing computation.
 * - product: the target product for which the price was calculated.
 * - newPrice: the final price to be published.
 */
@Value
public class PricingResult {
    Product product;
    Money newPrice;
}
