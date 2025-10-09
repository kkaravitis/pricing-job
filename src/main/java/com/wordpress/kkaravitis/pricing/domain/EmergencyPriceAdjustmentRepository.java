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
 * Port for reading any recent “emergency” adjustment factor for a product
 * (e.g. flash‐sale multiplier).
 */
public interface EmergencyPriceAdjustmentRepository extends Serializable {
    /**
     * @return a multiplier ≥1.0 if an emergency adjustment is active,
     *         or 1.0 if none.
     */
    double getAdjustmentFactor(String productId) throws PricingException;
}
