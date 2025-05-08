package com.mycompany.pricing.domain;

import com.mycompany.pricing.domain.PriceRule;
import java.io.Serializable;

/**
 * Port interface for fetching business price rules (min/max per SKU).
 * Implementations could read from broadcast state or configuration stores.
 */
public interface PriceRuleRepository extends Serializable {
    PriceRule getPriceRule(String productId) throws Exception;
}
