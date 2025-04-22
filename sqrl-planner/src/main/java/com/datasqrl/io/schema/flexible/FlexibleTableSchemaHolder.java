package com.datasqrl.io.schema.flexible;

import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.io.schema.flexible.external.SchemaExport;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

@Value
@AllArgsConstructor
public class FlexibleTableSchemaHolder implements TableSchema {

  @NonNull FlexibleTableSchema schema;
  String definition;
  @NonNull Optional<Path> location;

  public FlexibleTableSchemaHolder(FlexibleTableSchema schema) {
    this(schema, null, Optional.empty());
  }

  @Override
  public String getSchemaType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  @SneakyThrows
  public String getDefinition() {
    if (Strings.isNullOrEmpty(definition)) {
      SchemaExport schemaExport = new SchemaExport();
      TableDefinition schemaDef = schemaExport.export(schema);
      return Deserializer.INSTANCE.writeYML(schemaDef);
    }
    return definition;
  }

}
