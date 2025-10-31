/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.domain;

/**
 * Encapsulates the outcome of the pricing computation.
 */
public record PricingResult(String productId,
                            String productName,
                            Money newPrice,
                            Money modelPrediction,
                            Long timestamp,
                            Integer inventoryLevel,
                            Double currentDemand
                            ) {

}
