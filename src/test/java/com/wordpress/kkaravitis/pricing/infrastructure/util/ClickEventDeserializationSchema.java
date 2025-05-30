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
