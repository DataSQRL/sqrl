package com.datasqrl.io.schema.flexible;

import com.datasqrl.io.schema.flexible.external.SchemaExport;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Strings;
import java.net.URI;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

@Value
@AllArgsConstructor
public class FlexibleTableSchemaHolder implements TableSchema {

  @NonNull FlexibleTableSchema schema;
  String definition;
  @NonNull Optional<URI> location;

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
      return new Deserializer().writeYML(schemaDef);
    }
    return definition;
  }

}
