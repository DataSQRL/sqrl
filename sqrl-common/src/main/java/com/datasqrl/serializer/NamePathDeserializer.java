package com.datasqrl.serializer;

import com.datasqrl.canonicalizer.NamePath;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;

public class NamePathDeserializer extends JsonDeserializer<NamePath> {

  @Override
  public NamePath deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    String namePathString = jsonParser.getValueAsString();

    if (namePathString != null && !namePathString.isEmpty()) {
      return NamePath.parse(namePathString);
    } else {
      return null;
    }
  }
}
