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

/**
 * Port interface for ML model inference.
 * Takes a PricingContext and returns a model-suggested price wrapped in Money.
 */
public interface ModelInferencePricePredictor extends Serializable {
    Money predictPrice(PricingContext context) throws PricingException;
}

