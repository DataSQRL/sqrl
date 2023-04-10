package com.datasqrl.util.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;

import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.*;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.binary.Base64;

public class Base64Deserializer<T> extends StdDeserializer<T> {

    protected Base64Deserializer(Class<T> vc) {
        super(vc);
    }

    @Override
    public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String base64 = p.getValueAsString();
        byte[] bytes = Base64.decodeBase64(base64);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize object", e);
        }
    }
}