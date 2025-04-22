package com.datasqrl.io.schema.flexible;

import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.external.SchemaImport;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.serializer.Deserializer;
import com.google.auto.service.AutoService;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {
  public static final String SCHEMA_EXTENSION = ".schema.yml";

  public static final String SCHEMA_TYPE = "flexible";


  @Override
  public FlexibleTableSchemaHolder create(String schemaDefinition, Optional<Path> location, ErrorCollector errors) {
    var deserializer = Deserializer.INSTANCE;
    var schemaDef = deserializer.mapYAML(schemaDefinition, TableDefinition.class);
    var importer = new SchemaImport(Constraint.FACTORY_LOOKUP, NameCanonicalizer.SYSTEM);
    var tableSchema = importer.convert(schemaDef, errors).get();
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
