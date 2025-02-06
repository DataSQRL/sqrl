package com.datasqrl.format;

import java.util.Map;

import org.apache.flink.api.common.serialization.DeserializationSchema;

import com.datasqrl.io.tables.SchemaValidator;

import lombok.SneakyThrows;

public class JsonFlexibleSchemaDelegate extends FlexibleSchemaDelegate {

  public JsonFlexibleSchemaDelegate(
      DeserializationSchema schema,
      SchemaValidator validator) {
    super(schema, validator);
  }

  @SneakyThrows
  @Override
  public Map<String, Object> parse(byte[] message) {
    return objectMapper.readValue(message, Map.class);
  }
}
