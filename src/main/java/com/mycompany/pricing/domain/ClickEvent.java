package com.mycompany.pricing.domain;

import lombok.Getter;
import lombok.Value;

/**
 * Represents a user interaction event for a specific product.
 * Used as the primary input event in the pricing pipeline.
 */
@Getter
@Value
public class ClickEvent {
    String userId;
    String productId;
    long timestamp;
}