/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.AbstractExternalTable;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaImport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSource {

  public static final String TABLE_FILE_SUFFIX = ".table.json";
  public static final String DATASYSTEM_FILE = "datasystem.json";
  public static final String JSON_SCHEMA_FILE_SUFFIX = ".schema.json";
  public static final String PACKAGE_SCHEMA_FILE = "schema.yml";
  public static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.table\\.json$");

  protected static <T extends AbstractExternalTable> Optional<T> readTable(Path rootDir,
      NamePath fullPath,
      ErrorCollector errors, Class<T> clazz, Deserializer deserialize) {
    NamePath basePath = fullPath.subList(0, fullPath.size() - 1);
    String tableFileName = fullPath.getLast().getCanonical();
    Path baseDir = AbstractLoader.namepath2Path(rootDir, basePath);
    Path tableConfigPath = baseDir.resolve(tableFileName + TABLE_FILE_SUFFIX);
    //First, look for table specific schema file. If not present, look for package global schema file
    Path tableSchemaPath = baseDir.resolve(tableFileName + JSON_SCHEMA_FILE_SUFFIX);
    if (!Files.isRegularFile(tableSchemaPath)) {
      tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
    }
    if (!Files.isRegularFile(tableConfigPath)) {
      return Optional.empty();
    }

    TableConfig tableConfig = deserialize.mapJsonFile(tableConfigPath, TableConfig.class);
    if (tableConfig.getType() == null) {
      return Optional.empty(); //Invalid configuration
    }

    //Get table schema
    Optional<TableSchema> tableSchema = resolveSchema(deserialize, tableSchemaPath, tableConfig, errors);

    T resultTable;
    if (clazz.equals(TableSource.class)) {
      if (!tableConfig.getType().isSource()) {
        return Optional.empty();
      }
      //TableSource requires a schema
      if (tableSchema.isEmpty()) {
        log.warn("Found configuration for table [%s] but no schema. Table not loaded.", fullPath);
        return Optional.empty();
      }
      resultTable = (T) tableConfig.initializeSource(errors, basePath, tableSchema.get());
    } else if (clazz.equals(TableSink.class)) {
      if (!tableConfig.getType().isSink()) {
        return Optional.empty();
      }
      resultTable = (T) tableConfig.initializeSink(errors, basePath, tableSchema);
    } else {
      throw new UnsupportedOperationException("Invalid table clazz: " + clazz);
    }
    return Optional.of(resultTable);
  }

  private static Optional<TableSchema> resolveSchema(Deserializer deserialize, Path tableSchemaPath,
      TableConfig tableConfig, ErrorCollector errors) {

    if (Files.isRegularFile(tableSchemaPath)) {
      SchemaDefinition schemaDef = deserialize.mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
      SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP,
          tableConfig.getNameCanonicalizer());
      Map<Name, FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, errors);
      Preconditions.checkArgument(schemaDef.datasets.size() == 1);
      FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
      FlexibleDatasetSchema.TableField tbField = dsSchema.getFieldByName(
          tableConfig.getResolvedName());
      return Optional.of(tbField);
    }

    return Optional.empty();
  }
}