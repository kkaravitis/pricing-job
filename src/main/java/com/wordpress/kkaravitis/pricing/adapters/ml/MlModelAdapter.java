/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.adapters.ml;

import com.wordpress.kkaravitis.pricing.domain.ModelInferencePricePredictor;
import com.wordpress.kkaravitis.pricing.domain.Money;
import com.wordpress.kkaravitis.pricing.domain.PricingContext;
import com.wordpress.kkaravitis.pricing.domain.PricingException;
import java.io.Serial;
import java.io.Serializable;
import lombok.NoArgsConstructor;

/**
 * ModelInferencePricePredictor that wraps a deserialized TransformedModel.
 * You must call `updateModelBytes` when new bytes arrive.
 */
@NoArgsConstructor
public class MlModelAdapter implements ModelInferencePricePredictor, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient byte[] modelBytes;
    private transient TransformedModel model;
    private transient ModelDeserializer modelDeserializer;

    public void initialize() {
        this.modelDeserializer = new ModelDeserializer();
    }

    /**
     * Called by your Flink operator whenever new bytes arrive.
     **/
    public void updateModelBytes(byte[] bytes) {
        this.modelBytes = bytes;
        this.model = null; // force re-deserialize on next predict
    }

    @Override
    public Money predictPrice(PricingContext context) throws PricingException {
        if (model == null) {
            if (modelBytes == null) {
                throw new PricingException("Model bytes not initialized");
            }
            model = modelDeserializer.deserialize(modelBytes);
        }
        return model.predict(context);
    }
}
