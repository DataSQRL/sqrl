package com.datasqrl.util.serializer;

import com.datasqrl.name.Name;
import com.datasqrl.spi.JacksonDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.ServiceLoader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;

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

    addDeserializer(Name.class, new Name.NameDeserializer());
    addSerializer(Name.class, new Name.NameSerializer());

    addDeserializer(TypeInformation.class, new TypeInformationDeserializer());
    addSerializer(TypeInformation.class, new TypeInformationSerializer());

    addDeserializer(Schema.class, new SchemaDeserializer());
    addSerializer(Schema.class, new SchemaSerializer());
  }
}
