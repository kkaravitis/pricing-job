package com.wordpress.kkaravitis.pricing.adapters.ml;

import com.wordpress.kkaravitis.pricing.domain.ModelInferencePricePredictor;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingContext;
import java.io.Serializable;

/**
 * ModelInferencePort that wraps a deserialized TransformedModel.
 * You must call `updateModelBytes` when new bytes arrive.
 */
public class MlModelAdapter implements ModelInferencePricePredictor, Serializable {
    private static final long serialVersionUID = 1L;

    private final String modelKey;
    private transient byte[] modelBytes;
    private transient TransformedModel model;

    public MlModelAdapter(String modelKey) {
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
