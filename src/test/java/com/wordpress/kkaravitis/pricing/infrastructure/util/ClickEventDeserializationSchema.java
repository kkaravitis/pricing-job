/*
 * Copyright (c) 2025 Konstantinos Karavitis
 *
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 International License (CC BY-NC 4.0).
 * You may not use this file for commercial purposes.
 * See the LICENSE file in the project root or visit:
 * https://creativecommons.org/licenses/by-nc/4.0/
 */

package com.wordpress.kkaravitis.pricing.infrastructure.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;

public class ClickEventDeserializationSchema implements DeserializationSchema<ClickEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public ClickEvent deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;                        // or throw if you prefer
        }
        return MAPPER.readValue(message, ClickEvent.class);
    }

    @Override
    public boolean isEndOfStream(ClickEvent nextElement) {
        return false;                           // streaming never ends
    }

    @Override
    public TypeInformation<ClickEvent> getProducedType() {
        return Types.POJO(ClickEvent.class);    // or TypeInformation.of(new TypeHint<>() {})
    }
}
