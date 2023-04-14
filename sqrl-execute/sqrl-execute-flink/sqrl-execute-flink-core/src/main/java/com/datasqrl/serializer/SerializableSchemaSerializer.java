package com.datasqrl.serializer;

import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.service.AutoService;

@AutoService(StdSerializer.class)
public class SerializableSchemaSerializer extends Base64Serializer<SerializableSchema> {

  public SerializableSchemaSerializer() {
    super(SerializableSchema.class);
  }
}
