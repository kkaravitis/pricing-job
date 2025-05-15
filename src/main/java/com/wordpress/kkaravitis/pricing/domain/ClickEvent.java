package com.wordpress.kkaravitis.pricing.domain;

import lombok.Value;

/**
 * Represents a user interaction event for a specific product.
 * Used as the primary input event in the pricing pipeline.
 */
@Value
public class ClickEvent {
    String userId;
    String productId;
    long timestamp;
}