package com.mycompany.pricing.domain.port;

import com.mycompany.pricing.domain.model.PriceRule;
import java.io.Serializable;

/**
 * Port interface for fetching business price rules (min/max per SKU).
 * Implementations could read from broadcast state or configuration stores.
 */
public interface PriceRuleProvider extends Serializable {
    PriceRule getPriceRule(String productId) throws Exception;
}
