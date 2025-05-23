package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

/**
 * Port interface for fetching current inventory levels of a product.
 * Implementations could use Flink state, database lookup, or external API.
 */
public interface InventoryLevelRepository extends Serializable {
    int getInventoryLevel(String productId) throws PricingException;
}
