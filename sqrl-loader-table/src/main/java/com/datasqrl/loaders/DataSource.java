/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.AbstractExternalTable;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.NamePath;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSource {

  public static final String TABLE_FILE_SUFFIX = ".table.json";
  public static final String DATASYSTEM_FILE = "datasystem.json";
  public static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.table\\.json$");

  protected <T extends AbstractExternalTable> Optional<T> readTable(Path rootDir,
      NamePath fullPath,
      ErrorCollector errors, Class<T> clazz, Deserializer deserialize) {
    NamePath basePath = fullPath.subList(0, fullPath.size() - 1);
    String tableFileName = fullPath.getLast().getCanonical();
    Path baseDir = AbstractLoader.namepath2Path(rootDir, basePath);
    Path tableConfigPath = baseDir.resolve(tableFileName + TABLE_FILE_SUFFIX);
    //First, look for table specific schema file. If not present, look for package global schema file
    if (!Files.isRegularFile(tableConfigPath)) {
      return Optional.empty();
    }

    TableConfig tableConfig = deserialize.mapJsonFile(tableConfigPath, TableConfig.class);
    if (tableConfig.getType() == null) {
      return Optional.empty(); //Invalid configuration
    }

    //Get table schema
    Optional<TableSchema> tableSchema = resolveSchema(deserialize, baseDir, tableFileName, tableConfig, errors);

    T resultTable;
    if (clazz.equals(TableSource.class)) {
      if (!tableConfig.getType().isSource()) {
        return Optional.empty();
      }
      //TableSource requires a schema
      if (tableSchema.isEmpty()) {
        log.warn("Found configuration for table [{}] but no schema. Table not loaded.", fullPath);
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
    System.out.println(errors.getAll());
    return Optional.of(resultTable);
  }

  private Optional<TableSchema> resolveSchema(Deserializer deserialize, Path baseDir, String tableFileName,
      TableConfig tableConfig, ErrorCollector errors) {
    ServiceLoader<TableSchemaFactory> schemas = ServiceLoader.load(TableSchemaFactory.class);
    for (TableSchemaFactory factory : schemas) {
      if (Files.isRegularFile(baseDir.resolve(tableFileName + factory.baseFileSuffix()))) {
        return factory.create(deserialize, baseDir, tableConfig, errors);
      }
      if (factory.getFileName()
          .filter(f->Files.isRegularFile(baseDir.resolve(f))).isPresent()) {
        return factory.create(deserialize, baseDir, tableConfig, errors);
      }
    }

    return Optional.empty();
  }
}