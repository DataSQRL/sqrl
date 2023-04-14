package com.datasqrl.serializer;

import com.datasqrl.canonicalizer.Name;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.auto.service.AutoService;
import java.io.IOException;

@AutoService(JsonDeserializer.class)
public class NameDeserializer extends JsonDeserializer<Name> {

  @Override
  public Name deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException {
    String nameString = jsonParser.readValueAs(String.class);
    return Name.system(nameString);
  }
}
