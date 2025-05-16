package com.wordpress.kkaravitis.pricing.domain;

/**
 * Represents a product in the catalog identified by a unique productId. Used throughout the pricing engine to correlate streams of events and state.
 */
public record Product(String productId) {

}
