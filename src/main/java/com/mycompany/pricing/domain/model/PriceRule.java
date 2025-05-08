package com.mycompany.pricing.domain.model;

import java.io.Serializable;
import lombok.Value;

/**
 * Defines business-imposed pricing bounds for a product.
 * - minPrice: lowest allowed price set by category managers.
 * - maxPrice: highest allowed price set by category managers.
 * Ensures the computed price stays within safe thresholds.
 */
@Value
public class PriceRule implements Serializable {
    Money minPrice;
    Money maxPrice;
}