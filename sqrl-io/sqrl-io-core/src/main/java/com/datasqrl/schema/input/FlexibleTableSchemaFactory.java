package com.datasqrl.schema.input;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.BaseFileUtil;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.model.schema.TableDefinition;
import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Optional;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {
  public static final String SCHEMA_EXTENSION = ".schema.yml";

  public static final String SCHEMA_TYPE = "flexible";

  @Override
  public TableSchema create(NamePath basePath, URI baseURI, ResourceResolver resourceResolver, TableConfig tableConfig, ErrorCollector errors) {
    Optional<URI> schemaPath = resourceResolver
        .resolveFile(basePath.concat(NamePath.of(getSchemaFilename(tableConfig))));
    errors.checkFatal(schemaPath.isPresent(), "Could not find schema file [%s] for table [%s]", schemaPath, baseURI);
    return create(BaseFileUtil.readFile(schemaPath.get()), tableConfig.getBase().getCanonicalizer());
  }

  @Override
  public TableSchema create(String schemaDefinition, NameCanonicalizer nameCanonicalizer) {
    Deserializer deserializer = new Deserializer();
    TableDefinition schemaDef = deserializer.mapYAML(schemaDefinition, TableDefinition.class);
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, NameCanonicalizer.SYSTEM);
    FlexibleTableSchema tableSchema = importer.convert(schemaDef, ErrorCollector.root()).get();
    return new FlexibleTableSchemaHolder(tableSchema, schemaDefinition);
  }


  public static String getSchemaFilename(TableConfig tableConfig) {
    return tableConfig.getName().getCanonical() + SCHEMA_EXTENSION;
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }
}
