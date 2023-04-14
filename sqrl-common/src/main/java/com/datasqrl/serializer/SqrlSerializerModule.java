package com.datasqrl.serializer;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.ServiceLoader;
import org.apache.flink.api.common.typeinfo.TypeInformation;

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

    addDeserializer(Name.class, new NameDeserializer());
    addSerializer(Name.class, new NameSerializer());

    addDeserializer(NamePath.class, new NamePathDeserializer());
    addSerializer(NamePath.class, new NamePathSerializer());

    addDeserializer(TypeInformation.class, new TypeInformationDeserializer());
    addSerializer(TypeInformation.class, new TypeInformationSerializer());

    addDeserializer(SerializableSchema.class, new Base64Deserializer<>(SerializableSchema.class));
    addSerializer(SerializableSchema.class, new Base64Serializer<>(SerializableSchema.class));
  }
}
