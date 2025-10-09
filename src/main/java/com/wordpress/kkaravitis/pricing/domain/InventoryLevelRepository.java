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
 * Port interface for fetching current inventory levels of a product.
 * Implementations could use Flink state, database lookup, or external API.
 */
public interface InventoryLevelRepository extends Serializable {
    int getInventoryLevel(String productId) throws PricingException;
}
