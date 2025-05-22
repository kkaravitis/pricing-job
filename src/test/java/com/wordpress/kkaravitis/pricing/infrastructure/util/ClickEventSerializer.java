package com.wordpress.kkaravitis.pricing.infrastructure.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import com.wordpress.kkaravitis.pricing.domain.ClickEvent;

/**
 * Kafka value serializer that converts ClickEvent → UTF-8 JSON bytes.
 */
public final class ClickEventSerializer implements Serializer<ClickEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, ClickEvent data) {
        if (data == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize ClickEvent", e);
        }
    }
}
