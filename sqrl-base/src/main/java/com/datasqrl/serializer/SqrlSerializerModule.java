package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.util.ServiceLoader;

public class SqrlSerializerModule extends SimpleModule {

  public SqrlSerializerModule() {
    super();
    registerSqrlModules();
  }

  private void registerSqrlModules() {
    ServiceLoader<JacksonDeserializer> moduleLoader = ServiceLoader.load(JacksonDeserializer.class);
    for (JacksonDeserializer deserializer : moduleLoader) {
      super.addDeserializer(deserializer.getSuperType(), deserializer);
    }

    ServiceLoader<StdDeserializer> deserializers = ServiceLoader.load(StdDeserializer.class);
    for (StdDeserializer deserializer : deserializers) {
      super.addDeserializer(deserializer.getValueClass(), deserializer);
    }

    ServiceLoader<StdSerializer> serializers = ServiceLoader.load(StdSerializer.class);
    for (StdSerializer serializer : serializers) {
      super.addSerializer(serializer);
    }

   }
}
