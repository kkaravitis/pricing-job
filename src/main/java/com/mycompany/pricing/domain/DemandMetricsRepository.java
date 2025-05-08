package com.mycompany.pricing.domain;

import com.mycompany.pricing.domain.DemandMetrics;
import java.io.Serializable;

/**
 * Port interface for retrieving DemandMetrics for a product.
 * Implementations may aggregate real-time and historical data streams.
 */
public interface DemandMetricsRepository extends Serializable {
    DemandMetrics getDemandMetrics(String productId) throws Exception;//TODO: Replace with application dedicated exception
}