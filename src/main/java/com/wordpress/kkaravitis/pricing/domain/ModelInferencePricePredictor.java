package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Port interface for ML model inference.
 * Takes a PricingContext and returns a model-suggested price wrapped in Money.
 */
public interface ModelInferencePricePredictor extends Serializable {
    Money predictPrice(PricingContext context) throws Exception;//TODO: Replace with application dedicated exception
}

