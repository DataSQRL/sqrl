package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class SqrlSerializerModule extends SimpleModule {

  public SqrlSerializerModule() {
    super();
    registerSqrlModules();
  }

  private void registerSqrlModules() {
    List<JacksonDeserializer> jacksonDeserializers =
        cache(JacksonDeserializer.class);
    for (JacksonDeserializer deserializer : jacksonDeserializers) {
      super.addDeserializer(deserializer.getSuperType(), deserializer);
    }

    List<StdDeserializer> deserializers =
        cache(StdDeserializer.class);
    for (StdDeserializer deserializer : deserializers) {
      super.addDeserializer(deserializer.getValueClass(), deserializer);
    }

    List<StdSerializer> serializers =
        cache(StdSerializer.class);
    for (StdSerializer serializer : serializers) {
      super.addSerializer(serializer);
    }
   }
   private static final Map<Class, List<Object>> cache = new HashMap<>();
   private static <T> List<T> cache(Class<T> clazz) {
    if (cache.containsKey(clazz)) {
      return (List<T>)cache.get(clazz);
    }
    return ServiceLoader.load(clazz)
        .stream()
        .map(m->m.get())
        .collect(Collectors.toList());
  }
}
