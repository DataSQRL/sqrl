package com.datasqrl.schema.input;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.loaders.Deserializer;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.google.auto.service.AutoService;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {
  public static final String SCHEMA_EXTENSION = ".schema.yml";

  public static final String SCHEMA_TYPE = "flexible";

  @Override
  public Optional<TableSchema> create(URI baseURI, TableConfig tableConfig, Deserializer deserializer, ErrorCollector errors) {
    Path schemaPath = Path.of(baseURI.resolve(getSchemaFilename(tableConfig)));
    errors.checkFatal(Files.isRegularFile(schemaPath), "Could not find schema file [%s] for table [%s]", schemaPath, baseURI);
    TableDefinition schemaDef = deserializer.mapYAMLFile(schemaPath, TableDefinition.class);
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, tableConfig.getNameCanonicalizer());
    Optional<FlexibleTableSchema> tableSchema = importer.convert(schemaDef, errors );
    return tableSchema.map(f->f);
  }

  public static String getSchemaFilename(TableConfig tableConfig) {
    return tableConfig.getResolvedName().getCanonical() + SCHEMA_EXTENSION;
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }
}
