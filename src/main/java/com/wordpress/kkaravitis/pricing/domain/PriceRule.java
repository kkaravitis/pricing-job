/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Defines business-imposed pricing bounds for a product. - minPrice: lowest allowed price set by category managers. - maxPrice: highest allowed price set by
 * category managers. Ensures the computed price stays within safe thresholds.
 */
public record PriceRule(Money minPrice, Money maxPrice) implements Serializable {

    /**
     * Returns a rule that effectively imposes no bounds: min = 0, max = Double.MAX_VALUE in USD.
     */
    public static PriceRule defaults() {
        return new PriceRule(
              new Money(BigDecimal.ZERO, "EUR"),
              new Money(BigDecimal.valueOf(Double.MAX_VALUE), "EUR")
        );
    }

}