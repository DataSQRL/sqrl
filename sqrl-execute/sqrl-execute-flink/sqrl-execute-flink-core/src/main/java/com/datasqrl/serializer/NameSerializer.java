package com.datasqrl.serializer;

import com.datasqrl.canonicalizer.Name;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.auto.service.AutoService;
import java.io.IOException;

@AutoService(JsonSerializer.class)
public class NameSerializer extends JsonSerializer<Name> {

  @Override
  public void serialize(Name name, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException {
    jsonGenerator.writeString(name.getDisplay());
  }
}
