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

import com.wordpress.kkaravitis.pricing.domain.PricingRuntimeException;
import com.wordpress.kkaravitis.pricing.domain.PricingContext;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.Product;
import com.wordpress.kkaravitis.pricing.domain.DemandMetrics;
import com.wordpress.kkaravitis.pricing.domain.CompetitorPrice;
import com.wordpress.kkaravitis.pricing.domain.PriceRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class ModelDeserializerTest {

    private static byte[] validModelZip;
    private static byte[] invalidBytes = "not a zip".getBytes();

    @BeforeAll
    static void loadModelZip() throws Exception {
        try (InputStream in = ModelDeserializerTest.class.getResourceAsStream("/pricing_saved_model.zip")) {
            assertNotNull(in, "Place a valid model.zip under src/test/resources");
            validModelZip = in.readAllBytes();
        }
    }

    @Test
    void deserialize_withInvalidBytes_throws() {
        PricingRuntimeException ex = assertThrows(
              PricingRuntimeException.class,
              () -> new ModelDeserializer().deserialize(invalidBytes)
        );
        assertNotNull(ex.getCause());
    }


    @Test
    void deserialize_withValidZip_returnsWorkingModel() {
        TransformedModel model = new ModelDeserializer().deserialize(validModelZip);
        assertNotNull(model);

        PricingContext ctx = new PricingContext(
              new Product("p-001", "p-001"),
              new DemandMetrics("p-001", "p-001",2.5, 1.0),
              4,
              new CompetitorPrice("p-001", "p-001", new Money(35, "EUR")),
              PriceRule.defaults()
        );

        Money price = model.predict(ctx);
        assertTrue(price.getAmount().compareTo(BigDecimal.ZERO) > 0);
    }

    @Test
    void updateModelBytes_multipleTimes_resetsInternalModel() {
        ModelDeserializer deserializer = new ModelDeserializer();

        TransformedModel model1 = deserializer.deserialize(validModelZip);
        Money price1 = model1.predict(new PricingContext(
              new Product("p-001", ""),
              new DemandMetrics("p-001", "p-001",2.0, 1.0),
              10,
              new CompetitorPrice("p-001", "p-001", new Money(5.0, "EUR")),
              PriceRule.defaults()
        ));

        TransformedModel model2 = deserializer.deserialize(validModelZip);
        Money price2 = model2.predict(new PricingContext(
              new Product("p-001", ""),
              new DemandMetrics("p-001", "p-001",2.0, 1.0),
              10,
              new CompetitorPrice("p-001", "p-001", new Money(5.0, "EUR")),
              PriceRule.defaults()
        ));

        assertNotNull(price1);
        assertNotNull(price2);
        assertEquals(price1, price2, "Reloading the same model bytes should produce identical results");
    }
}