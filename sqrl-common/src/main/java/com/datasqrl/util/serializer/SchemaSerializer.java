package com.datasqrl.util.serializer;

import org.apache.flink.table.api.Schema;

public class SchemaSerializer extends Base64Serializer<Schema> {

  protected SchemaSerializer() {
    super(Schema.class);
  }
}
