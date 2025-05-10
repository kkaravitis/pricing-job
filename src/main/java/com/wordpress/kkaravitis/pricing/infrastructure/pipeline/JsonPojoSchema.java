package com.wordpress.kkaravitis.pricing.infrastructure.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class JsonPojoSchema<T> implements SerializationSchema<T> {
    private final Class<T> clazz;
    private transient ObjectMapper mapper;

    public JsonPojoSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void open(InitializationContext context) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to JSON-serialize element: " + element, e);
        }
    }
}
