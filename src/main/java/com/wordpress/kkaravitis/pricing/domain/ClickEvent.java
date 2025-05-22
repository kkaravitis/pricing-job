package com.wordpress.kkaravitis.pricing.domain;

/**
 * Represents a user interaction event for a specific product.
 * Used as the primary input event in the pricing pipeline.
 */
public record ClickEvent (String productId, long timestamp) {}