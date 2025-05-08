package com.mycompany.pricing.domain.model;

import lombok.Value;

@Value
public class InventoryEvent {
    String productId;
    int level;
}
