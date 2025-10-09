/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.adapters.ml;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PriceRule;
import com.wordpress.kkaravitis.pricing.domain.PricingContext;
import com.wordpress.kkaravitis.pricing.domain.Product;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MlModelAdapterTest {

    private MlModelAdapter adapter;
    private PricingContext dummyCtx;

    @BeforeEach
    void setUp() {
        adapter = new MlModelAdapter();
        adapter.initialize();
        // re‐use same dummy context as above
        String pid = "p1";
        dummyCtx = new PricingContext(
              new Product(pid, pid),
              new DemandMetrics(pid, pid,2.0, 1.0),
              3,
              new CompetitorPrice(pid, pid, new Money(2.0, "EUR")),
              PriceRule.defaults()
        );
    }

    @Test
    void predict_beforeUpdate_throwsIllegalState() {
        IllegalStateException ex = assertThrows(
              IllegalStateException.class,
              () -> adapter.predictPrice(dummyCtx)
        );
        assertTrue(ex.getMessage().contains("Model bytes not initialized"));
    }
}