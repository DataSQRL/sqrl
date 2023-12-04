package com.datasqrl.schema.input;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.SchemaValidator;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.converters.FlexibleSchemaRowMapper;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.engine.stream.RowMapper;
import com.datasqrl.schema.input.external.SchemaExport;
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
  public RowMapper getRowMapper(RowConstructor rowConstructor,
      DataSystemConnectorSettings connectorSettings) {
    return new FlexibleSchemaRowMapper(schema, connectorSettings.isHasSourceTimestamp(),
        rowConstructor);
  }

  @Override
  public String getSchemaType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public SchemaValidator getValidator(SchemaAdjustmentSettings schemaAdjustmentSettings,
      DataSystemConnectorSettings connectorSettings) {
    FlexibleSchemaValidator validator = new FlexibleSchemaValidator(schema,
        connectorSettings.isHasSourceTimestamp(),
        schemaAdjustmentSettings,
        NameCanonicalizer.SYSTEM,
        new FlexibleTypeMatcher(schemaAdjustmentSettings));
    return validator;
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
