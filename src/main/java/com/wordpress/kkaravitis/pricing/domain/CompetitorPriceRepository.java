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
 * Port interface for retrieving competitor pricing data for a product.
 * Implementations may perform async HTTP calls or read from Kafka sources.
 */
public interface CompetitorPriceRepository extends Serializable {
    CompetitorPrice getCompetitorPrice(String productId) throws PricingException;
}