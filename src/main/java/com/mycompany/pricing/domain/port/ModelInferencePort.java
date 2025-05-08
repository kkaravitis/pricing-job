package com.mycompany.pricing.domain.port;

import com.mycompany.pricing.domain.model.PricingContext;
import com.mycompany.pricing.domain.model.Money;
import java.io.Serializable;

/**
 * Port interface for ML model inference.
 * Takes a PricingContext and returns a model-suggested price wrapped in Money.
 */
public interface ModelInferencePort extends Serializable {
    Money predictPrice(PricingContext context) throws Exception;
}

