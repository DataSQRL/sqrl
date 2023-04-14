package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.auto.service.AutoService;

@AutoService(StdDeserializer.class)
public class SerializableSchemaDeserializer extends Base64Deserializer<SerializableSchema> {

  public SerializableSchemaDeserializer() {
    super(SerializableSchema.class);
  }
}
