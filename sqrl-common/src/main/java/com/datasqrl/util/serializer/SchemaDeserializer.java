package com.datasqrl.util.serializer;

import org.apache.flink.table.api.Schema;

public class SchemaDeserializer extends Base64Deserializer<Schema> {

  protected SchemaDeserializer() {
    super(Schema.class);
  }
}
