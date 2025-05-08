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
