package com.datasqrl.io.schema.flexible;

import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.io.schema.flexible.external.SchemaImport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Optional;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {
  public static final String SCHEMA_EXTENSION = ".schema.yml";

  public static final String SCHEMA_TYPE = "flexible";


  @Override
  public FlexibleTableSchemaHolder create(String schemaDefinition, Optional<URI> location, ErrorCollector errors) {
    Deserializer deserializer = new Deserializer();
    TableDefinition schemaDef = deserializer.mapYAML(schemaDefinition, TableDefinition.class);
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, NameCanonicalizer.SYSTEM);
    FlexibleTableSchema tableSchema = importer.convert(schemaDef, errors).get();
    return new FlexibleTableSchemaHolder(tableSchema, schemaDefinition, location);
  }


  @Override
  public String getSchemaFilename(TableConfig tableConfig) {
    return tableConfig.getName().getDisplay() + SCHEMA_EXTENSION;
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }

  @Override
  public String getExtension() {
    return SCHEMA_EXTENSION;
  }
}
