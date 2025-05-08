package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Port interface for retrieving DemandMetrics for a product.
 * Implementations may aggregate real-time and historical data streams.
 */
public interface DemandMetricsRepository extends Serializable {
    DemandMetrics getDemandMetrics(String productId) throws Exception;//TODO: Replace with application dedicated exception
}