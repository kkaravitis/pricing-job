package com.wordpress.kkaravitis.pricing.domain;

import lombok.Getter;
import lombok.Value;

/**
 * Represents an order placed for a specific product.
 */
@Getter
@Value
public class OrderEvent {
    String orderId;
    String productId;
    int quantity;
    long timestamp;
}
