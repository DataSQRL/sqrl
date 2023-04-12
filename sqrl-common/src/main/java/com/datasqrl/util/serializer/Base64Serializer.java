package com.datasqrl.util.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.*;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.binary.Base64;

public abstract class Base64Serializer<T extends Serializable> extends StdSerializer<T> {

    protected Base64Serializer(Class<T> t) {
        super(t);
    }

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        byte[] bytes = baos.toByteArray();
        String base64 = Base64.encodeBase64String(bytes);
        gen.writeString(base64);
    }
}