package com.datasqrl.error;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

public class ErrorMessageSerializer extends StdSerializer<ErrorMessage> {

  public ErrorMessageSerializer() {
    this(null);
  }

  public ErrorMessageSerializer(Class<ErrorMessage> t) {
    super(t);
  }

  @Override
  public void serialize(ErrorMessage msg, JsonGenerator jgen, SerializerProvider serializerProvider)
      throws IOException {
  }
}
