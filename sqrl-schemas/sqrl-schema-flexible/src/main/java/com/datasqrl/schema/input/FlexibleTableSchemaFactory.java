package com.datasqrl.schema.input;

import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.loaders.Deserializer;
import com.datasqrl.name.Name;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaImport;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;

@AutoService(TableSchemaFactory.class)
public class FlexibleTableSchemaFactory implements TableSchemaFactory {
  public static final String PACKAGE_SCHEMA_FILE = "schema.yml";

  public String baseFileSuffix() {
    return "." + PACKAGE_SCHEMA_FILE;
  }

  @Override
  public Set<String> allSuffixes() {
    return Set.of(PACKAGE_SCHEMA_FILE);
  }

  @Override
  public Optional<String> getFileName() {
    return Optional.of(PACKAGE_SCHEMA_FILE);
  }


  @SneakyThrows
  public Optional<TableSchema> create(URL url, SchemaFactoryContext context) {
    SchemaDefinition schemaDef = new Deserializer().mapYAMLFile(Path.of(url.toURI()), SchemaDefinition.class);

    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP,
        context.getCanonicalizer());
    Map<Name, FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, context.getErrors());
    Preconditions.checkArgument(schemaDef.datasets.size() == 1);
    FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
    Optional<FlexibleDatasetSchema.TableField> tbField = dsSchema.getFieldByName(
        context.getResolvedName());
    return tbField.map(f->f);
  }

//
//  @Override
//  public Optional<TableSchema> create(Deserializer deserialize, Path baseDir, TableConfig tableConfig,
//      ErrorCollector errors) {
//    Path tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
//
//    SchemaDefinition schemaDef = deserialize.mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
//
//    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP,
//        tableConfig.getNameCanonicalizer());
//    Map<Name, FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, errors);
//    Preconditions.checkArgument(schemaDef.datasets.size() == 1);
//    FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
//    Optional<FlexibleDatasetSchema.TableField> tbField = dsSchema.getFieldByName(
//        tableConfig.getResolvedName());
//    return tbField.map(f->f);
//  }
}
