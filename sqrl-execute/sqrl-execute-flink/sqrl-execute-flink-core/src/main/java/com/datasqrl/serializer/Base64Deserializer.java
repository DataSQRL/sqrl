package com.datasqrl.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;

import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.*;
import org.apache.commons.codec.binary.Base64;

public abstract class Base64Deserializer<T extends Serializable> extends StdDeserializer<T> {

  public Base64Deserializer(Class<T> vc) {
    super(vc);
  }

  @Override
  public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    String base64 = p.getValueAsString();
    byte[] bytes = Base64.decodeBase64(base64);
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new CustomObjectInputStream(bais, contextClassLoader)) {
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to deserialize object", e);
    }
  }

  public class CustomObjectInputStream extends ObjectInputStream {

    private final ClassLoader classLoader;

    public CustomObjectInputStream(ByteArrayInputStream bais, ClassLoader classLoader)
        throws IOException {
      super(bais);
      this.classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
        throws IOException, ClassNotFoundException {
      try {
        return Class.forName(desc.getName(), false, classLoader);
      } catch (ClassNotFoundException e) {
        return super.resolveClass(desc);
      }
    }
  }
}