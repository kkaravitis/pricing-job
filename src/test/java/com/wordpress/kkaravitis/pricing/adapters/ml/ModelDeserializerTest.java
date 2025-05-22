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

//    @BeforeAll
//    static void loadModelZip() throws Exception {
//        try (InputStream in = ModelDeserializerTest.class.getResourceAsStream("/model.zip")) {
//            assertNotNull(in, "Place a valid model.zip under src/test/resources");
//            validModelZip = in.readAllBytes();
//        }
//    }

//    @Test
//    void deserialize_withInvalidBytes_throws() {
//        PricingRuntimeException ex = assertThrows(
//              PricingRuntimeException.class,
//              () -> ModelDeserializer.deserialize(invalidBytes)
//        );
//        assertTrue(ex.getMessage().contains("Failed to load TensorFlow model"));
//        assertNotNull(ex.getCause());
//    }


//    @Test
//    void deserialize_withValidZip_returnsWorkingModel() throws Exception {
//        TransformedModel model = ModelDeserializer.deserialize(validModelZip);
//        assertNotNull(model);
//
//        // Prepare a fake context: model = sum(features)+1
//        String pid = "p1";
//        PricingContext ctx = new PricingContext(
//              new Product(pid),
//              new DemandMetrics(pid, 2.5, 1.0),
//              4,
//              new CompetitorPrice(pid, new Money(3.5, "USD")),
//              PriceRule.defaults()
//        );
//
//        Money price = model.predict(ctx);
//        // expected = inventory(4) + currentDemand(2.5) + competitor(3.5) + 1 = 11.0
//        assertEquals(new Money(BigDecimal.valueOf(11.0), "USD"), price);
//    }
}