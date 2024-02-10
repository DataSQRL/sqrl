package com.datasqrl.schema.converters;

import com.datasqrl.io.tables.SchemaValidator;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public class JsonFlexibleSchemaDelegate extends FlexibleSchemaDelegate {

  public JsonFlexibleSchemaDelegate(
      DeserializationSchema schema,
      SchemaValidator validator) {
    super(schema, validator);
  }

  @SneakyThrows
  @Override
  public Map<String, Object> parse(byte[] message) {
    Map map = objectMapper.readValue(message, Map.class);
    return map;
  }
}
