package com.mycompany.pricing.domain.port;

import com.mycompany.pricing.domain.model.CompetitorPrice;
import java.io.Serializable;

/**
 * Port interface for retrieving competitor pricing data for a product.
 * Implementations may perform async HTTP calls or read from Kafka sources.
 */
public interface CompetitorPriceProvider extends Serializable {
    CompetitorPrice getCompetitorPrice(String productId) throws Exception;
}