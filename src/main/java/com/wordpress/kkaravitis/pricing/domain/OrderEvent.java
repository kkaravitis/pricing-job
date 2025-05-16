package com.wordpress.kkaravitis.pricing.domain;

/**
 * Represents an order placed for a specific product.
 */
public record OrderEvent(String orderId, String productId, int quantity, long timestamp) {

}
