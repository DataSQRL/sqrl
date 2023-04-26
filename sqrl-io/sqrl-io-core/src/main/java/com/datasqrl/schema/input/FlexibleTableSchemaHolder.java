package com.datasqrl.schema.input;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.model.schema.SchemaDefinition;
import com.datasqrl.schema.converters.FlexibleSchemaRowMapper;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Strings;
import com.datasqrl.schema.input.external.SchemaExport;
import lombok.*;

@Value
@AllArgsConstructor
public class FlexibleTableSchemaHolder implements TableSchema {

  @NonNull FlexibleTableSchema schema;
  String definition;

  public FlexibleTableSchemaHolder(FlexibleTableSchema schema) {
    this(schema, null);
  }

  @Override
  public Name getTableName() {
    return schema.getName();
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
      SchemaDefinition schemaDef = schemaExport.export(schema);
      return new Deserializer().writeYML(schemaDef);
    }
    return definition;
  }

}
