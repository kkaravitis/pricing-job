package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * A wrapper for any of the feeder‐pipeline metric types,
 * so they can be unioned into a single keyed stream.
 *
 * @param productId the key for this metric
 * @param type      which metric this is
 * @param payload   the raw metric object (DemandMetrics, InventoryLevel, etc.)
 */
public record MetricUpdate(String productId,
    MetricType type, Serializable payload) implements Serializable {

}
