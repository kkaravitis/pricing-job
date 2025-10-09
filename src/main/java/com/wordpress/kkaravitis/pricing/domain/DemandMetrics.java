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
 * Holds metrics related to product demand. - currentDemand: the real-time demand rate (e.g., orders or clicks per minute). - historicalAverage: baseline demand
 * derived from historical data. These values inform pricing adjustments based on demand fluctuations.
 */
public record DemandMetrics(String productId, String productName, double currentDemand, double historicalAverage) implements Serializable {
}