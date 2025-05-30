/*
 * Copyright 2025 Konstantinos Karavitis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
              new Product(pid),
              new DemandMetrics(pid, 2.0, 1.0),
              3,
              new CompetitorPrice(pid, new Money(2.0, "USD")),
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

//    @Test
//    void predict_afterInvalidBytes_throwsRuntime() {
//        adapter.updateModelBytes("garbage".getBytes());
//        PricingRuntimeException ex = assertThrows(
//              PricingRuntimeException.class,
//              () -> adapter.predictPrice(dummyCtx)
//        );
//        assertTrue(ex.getMessage().contains("Failed to load TensorFlow model"));
//    }

//    @Test
//    void updateModelBytes_multipleTimes_resetsInternalModel() throws Exception {
//        // First set valid bytes → predict succeeds
//        byte[] valid = ModelDeserializerTest.validModelZip;  // assume same resource
//        adapter.updateModelBytes(valid);
//        Money first = adapter.predictPrice(dummyCtx);
//        assertNotNull(first);
//
//        // Then set invalid bytes → next predict fails
//        adapter.updateModelBytes("oops".getBytes());
//        assertThrows(PricingRuntimeException.class,
//              () -> adapter.predictPrice(dummyCtx));
//
//        // Finally clear to null via new instance → IllegalState
//        adapter = new MlModelAdapter("test-model");
//        assertThrows(IllegalStateException.class,
//              () -> adapter.predictPrice(dummyCtx));
//    }
}