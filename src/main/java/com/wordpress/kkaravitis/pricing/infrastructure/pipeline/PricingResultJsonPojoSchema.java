/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.PricingResult;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@NoArgsConstructor
public final class PricingResultJsonPojoSchema implements SerializationSchema<PricingResult> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(PricingResult element) {
        try {
            return MAPPER.writeValueAsBytes(element);
        } catch (Exception exception) {
            throw new RuntimeException("Failed to serialize " +
                  PricingResult.class.getSimpleName(), exception);
        }
    }
}
