package com.datasqrl.io.schema.avro;

import com.datasqrl.io.tables.TableSchema;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.avro.Schema;

@Value
@AllArgsConstructor
public class AvroSchemaHolder implements TableSchema {

  Schema schema;
  String schemaDefinition;
  Optional<Path> location;

  @Override
  public String getSchemaType() {
    return AvroTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public String getDefinition() {
    return schemaDefinition;
  }
}
