package com.mycompany.pricing.domain.model;

import lombok.Value;

/**
 * Holds metrics related to product demand.
 * - currentDemand: the real-time demand rate (e.g., orders or clicks per minute).
 * - historicalAverage: baseline demand derived from historical data.
 * These values inform pricing adjustments based on demand fluctuations.
 */
@Value
public class DemandMetrics {
    String productId;
    double currentDemand;
    double historicalAverage;
}