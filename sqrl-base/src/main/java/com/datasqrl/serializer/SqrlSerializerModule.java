package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
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
    ServiceLoader<JacksonDeserializer> jacksonDeserializers = ServiceLoader.load(JacksonDeserializer.class);
    for (JacksonDeserializer deserializer : jacksonDeserializers) {
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
//
//    ServiceLoader<JsonDeserializer> jsonDeserializer = ServiceLoader.load(JsonDeserializer.class);
//    for (JsonDeserializer deserializer : jsonDeserializer) {
//      super.addDeserializer(deserializer.getClass(), deserializer);
//    }
//
//    ServiceLoader<JsonSerializer> jsonSerializer = ServiceLoader.load(JsonSerializer.class);
//    for (JsonSerializer serializer : jsonSerializer) {
//      super.addSerializer(serializer);
//    }

   }
}
