package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Holds metrics related to product demand. - currentDemand: the real-time demand rate (e.g., orders or clicks per minute). - historicalAverage: baseline demand
 * derived from historical data. These values inform pricing adjustments based on demand fluctuations.
 */
public record DemandMetrics(String productId, double currentDemand, double historicalAverage) implements Serializable {
}