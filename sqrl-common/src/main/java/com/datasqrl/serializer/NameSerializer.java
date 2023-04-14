package com.datasqrl.serializer;

import com.datasqrl.canonicalizer.Name;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class NameSerializer extends JsonSerializer<Name> {

  @Override
  public void serialize(Name name, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException {
    jsonGenerator.writeString(name.getDisplay());
  }
}
