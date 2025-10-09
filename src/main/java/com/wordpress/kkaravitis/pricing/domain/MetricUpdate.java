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
