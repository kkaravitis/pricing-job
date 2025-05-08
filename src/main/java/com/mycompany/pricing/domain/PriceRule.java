package com.mycompany.pricing.domain;

import java.io.Serializable;
import java.math.BigDecimal;
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

    /**
     * Returns a rule that effectively imposes no bounds:
     * min = 0, max = Double.MAX_VALUE in USD.
     */
    public static PriceRule defaults() {
        return new PriceRule(
              new Money(BigDecimal.ZERO, "USD"),
              new Money(BigDecimal.valueOf(Double.MAX_VALUE), "USD")
        );
    }

}