package com.mycompany.pricing.domain.model;

import lombok.Value;

/**
 * Represents a product in the catalog identified by a unique productId.
 * Used throughout the pricing engine to correlate streams of events and state.
 */
@Value
public class Product {
    String productId;
}
