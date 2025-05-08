package com.mycompany.pricing.infrastructure.source;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * A Flink DeserializationSchema that emits the raw Kafka value bytes as-is.
 */
public class RawByteDeserializationSchema implements DeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] message)  {
        // message is already the raw bytes
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        // Byte array type
        return TypeExtractor.getForClass(byte[].class);
    }
}
