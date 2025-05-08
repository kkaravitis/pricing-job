package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Port interface for fetching business price rules (min/max per SKU).
 * Implementations could read from broadcast state or configuration stores.
 */
public interface PriceRuleRepository extends Serializable {
    PriceRule getPriceRule(String productId) throws Exception;//TODO: Replace with application dedicated exception
}
