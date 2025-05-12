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
