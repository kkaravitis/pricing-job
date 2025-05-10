package com.wordpress.kkaravitis.pricing.domain;

import lombok.Value;

/**
 * Emitted whenever a flash-sale or spike is detected.
 * adjustmentFactor >1.0 means we increase price by that factor.
 */
@Value
public class EmergencyPriceAdjustment {
    String productId;
    double adjustmentFactor;
}
