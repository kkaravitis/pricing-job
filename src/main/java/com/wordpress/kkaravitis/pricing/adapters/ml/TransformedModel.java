/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.adapters.ml;

import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingContext;
import java.io.Serializable;

/**
 * Abstraction over a deserialized ML model capable of scoring PricingContext.
 * Returns a Money object to preserve currency precision and context.
 */
public interface TransformedModel extends Serializable {
    /**
     * Predicts a price given the pricing context and returns it as Money.
     */
    Money predict(PricingContext context);
}
