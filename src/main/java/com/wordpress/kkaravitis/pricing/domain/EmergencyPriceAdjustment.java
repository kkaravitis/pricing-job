package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Emitted whenever a flash-sale or spike is detected. adjustmentFactor >1.0 means we increase price by that factor.
 */
public record EmergencyPriceAdjustment(String productId, double adjustmentFactor) implements Serializable {
}
