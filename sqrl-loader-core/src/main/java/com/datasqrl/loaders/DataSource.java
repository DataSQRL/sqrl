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
import com.datasqrl.name.NamePath;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSource {

  public static final String TABLE_FILE_SUFFIX = ".table.json";
  public static final String DATASYSTEM_FILE = "datasystem.json";
  public static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.table\\.json$");

  public <T extends AbstractExternalTable> Optional<T> readTable(Class<T> clazz,
      TableSchema tableSchema, TableConfig tableConfig, ErrorCollector errors,
      NamePath basePath) {
    T resultTable;
    if (clazz.equals(TableSource.class)) {
      if (!tableConfig.getType().isSource()) {
        return Optional.empty();
      }
      //TableSource requires a schema
      if (tableSchema == null) {
        log.warn("Found configuration for table [{}] but no schema. Table not loaded.",
            tableConfig.getResolvedName());
        return Optional.empty();
      }
      resultTable = (T) tableConfig.initializeSource(errors, basePath, tableSchema);
    } else if (clazz.equals(TableSink.class)) {
      if (!tableConfig.getType().isSink()) {
        return Optional.empty();
      }
      resultTable = (T) tableConfig.initializeSink(errors, basePath, Optional.of(tableSchema));
    } else {
      throw new UnsupportedOperationException("Invalid table clazz: " + clazz);
    }
    return Optional.of(resultTable);
  }
}