package com.wordpress.kkaravitis.pricing.domain;

import lombok.Value;

@Value
public class InventoryEvent {
    String productId;
    int level;
}
