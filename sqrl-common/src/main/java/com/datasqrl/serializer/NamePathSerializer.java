package com.datasqrl.serializer;

import com.datasqrl.canonicalizer.NamePath;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class NamePathSerializer extends JsonSerializer<NamePath> {

  @Override
  public void serialize(NamePath namePath, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider)
      throws IOException {
    if (namePath != null) {
      jsonGenerator.writeString(namePath.getDisplay());
    } else {
      jsonGenerator.writeNull();
    }
  }
}
