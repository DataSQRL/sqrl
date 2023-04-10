package com.datasqrl.schema.input;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.SchemaDefinition;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.util.serializer.Deserializer;
import com.datasqrl.loaders.ResourceResolver;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Optional;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {
  public static final String SCHEMA_EXTENSION = ".schema.yml";

  public static final String SCHEMA_TYPE = "flexible";

  @Override
  public Optional<TableSchema> create(NamePath basePath, URI baseURI, ResourceResolver resourceResolver, TableConfig tableConfig, Deserializer deserializer, ErrorCollector errors) {
    Optional<URI> schemaPath = resourceResolver
        .resolveFile(basePath.concat(NamePath.of(getSchemaFilename(tableConfig))));
    if (schemaPath.isEmpty()) {
      errors.fatal("Could not find schema file [%s] for table [%s]", schemaPath, baseURI);
    }
    TableDefinition schemaDef = deserializer.mapYAMLFile(schemaPath.get(), TableDefinition.class);
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, tableConfig.getNameCanonicalizer());
    Optional<FlexibleTableSchema> tableSchema = importer.convert(schemaDef, errors );
    return tableSchema.map(f->f);
  }

  @Override
  public Optional<TableSchema> create(SchemaDefinition definition) {
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, NameCanonicalizer.SYSTEM);
    return importer.convert((TableDefinition) definition, ErrorCollector.root())
        .map(s -> s);
  }

  public static String getSchemaFilename(TableConfig tableConfig) {
    return tableConfig.getResolvedName().getCanonical() + SCHEMA_EXTENSION;
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }
}
