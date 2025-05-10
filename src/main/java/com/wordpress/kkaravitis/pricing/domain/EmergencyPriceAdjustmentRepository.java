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
    double getAdjustmentFactor(String productId) throws Exception;
}
