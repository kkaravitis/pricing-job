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
 * Emitted whenever a flash-sale or spike is detected. adjustmentFactor >1.0 means we increase price by that factor.
 */
public record EmergencyPriceAdjustment(String productId, String productName, double adjustmentFactor) implements Serializable {
}
