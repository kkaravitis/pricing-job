package com.mycompany.pricing.domain;

import com.mycompany.pricing.domain.CompetitorPrice;
import java.io.Serializable;

/**
 * Port interface for retrieving competitor pricing data for a product.
 * Implementations may perform async HTTP calls or read from Kafka sources.
 */
public interface CompetitorPriceRepository extends Serializable {
    CompetitorPrice getCompetitorPrice(String productId) throws Exception; //TODO: Replace with application dedicated exception
}