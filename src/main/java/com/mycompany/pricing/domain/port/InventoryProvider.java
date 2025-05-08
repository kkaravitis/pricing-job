package com.mycompany.pricing.domain.port;

import java.io.Serializable;

/**
 * Port interface for fetching current inventory levels of a product.
 * Implementations could use Flink state, database lookup, or external API.
 */
public interface InventoryProvider extends Serializable {
    int getInventoryLevel(String productId) throws Exception;
}
