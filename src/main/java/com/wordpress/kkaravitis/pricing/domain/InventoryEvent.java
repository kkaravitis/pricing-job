package com.wordpress.kkaravitis.pricing.domain;

import java.io.Serializable;

public record InventoryEvent(String productId, int level) implements Serializable {
}
