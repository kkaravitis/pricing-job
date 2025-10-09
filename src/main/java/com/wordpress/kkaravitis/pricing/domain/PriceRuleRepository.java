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
 * Port interface for fetching business price rules (min/max per SKU).
 * Implementations could read from broadcast state or configuration stores.
 */
public interface PriceRuleRepository extends Serializable {
    PriceRule getPriceRule(String productId) throws PricingException;
}
