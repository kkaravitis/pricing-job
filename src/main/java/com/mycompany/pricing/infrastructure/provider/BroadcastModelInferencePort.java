// src/main/java/com/mycompany/pricing/infrastructure/provider/BroadcastModelInferencePort.java
package com.mycompany.pricing.infrastructure.provider;

import com.mycompany.pricing.domain.model.PricingContext;
import com.mycompany.pricing.domain.model.Money;
import com.mycompany.pricing.domain.port.ModelInferencePort;

import java.io.Serializable;

/**
 * ModelInferencePort that wraps a deserialized TransformedModel.
 * You must call `updateModelBytes` when new bytes arrive.
 */
public class BroadcastModelInferencePort implements ModelInferencePort, Serializable {
    private static final long serialVersionUID = 1L;

    private final String modelKey;
    private transient byte[] modelBytes;
    private transient TransformedModel model;

    public BroadcastModelInferencePort(String modelKey) {
        this.modelKey = modelKey;
    }

    /** Called by your Flink operator whenever new bytes arrive. */
    public void updateModelBytes(byte[] bytes) {
        this.modelBytes = bytes;
        this.model = null; // force re-deserialize on next predict
    }

    @Override
    public Money predictPrice(PricingContext context) {
        if (model == null) {
            if (modelBytes == null) {
                throw new IllegalStateException("Model bytes not initialized");
            }
            model = ModelDeserializer.deserialize(modelBytes);
        }
        return model.predict(context);
    }
}
