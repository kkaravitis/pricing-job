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
package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wordpress.kkaravitis.pricing.domain.PricingRuntimeException;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SerializationSchema;

@NoArgsConstructor
public class JsonPojoSchema<T> implements SerializationSchema<T> {

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new PricingRuntimeException("Failed to JSON-serialize element: " + element, e);
        }
    }
}
