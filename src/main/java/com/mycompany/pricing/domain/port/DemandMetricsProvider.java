package com.mycompany.pricing.domain.port;

import com.mycompany.pricing.domain.model.DemandMetrics;
import java.io.Serializable;

/**
 * Port interface for retrieving DemandMetrics for a product.
 * Implementations may aggregate real-time and historical data streams.
 */
public interface DemandMetricsProvider extends Serializable {
    DemandMetrics getDemandMetrics(String productId) throws Exception;
}