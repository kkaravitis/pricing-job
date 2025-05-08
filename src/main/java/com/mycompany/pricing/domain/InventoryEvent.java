package com.mycompany.pricing.domain;

import lombok.Value;

@Value
public class InventoryEvent {
    String productId;
    int level;
}
