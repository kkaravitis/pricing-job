package com.wordpress.kkaravitis.pricing.adapters.ml;

import com.wordpress.kkaravitis.pricing.domain.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void predict_afterInvalidBytes_throwsRuntime() {
        adapter.updateModelBytes("garbage".getBytes());
        PricingRuntimeException ex = assertThrows(
              PricingRuntimeException.class,
              () -> adapter.predictPrice(dummyCtx)
        );
        assertTrue(ex.getMessage().contains("Failed to load TensorFlow model"));
    }

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