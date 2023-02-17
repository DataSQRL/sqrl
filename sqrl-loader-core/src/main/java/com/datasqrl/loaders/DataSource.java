/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.*;
import com.datasqrl.name.NamePath;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class DataSource {

  public static final String TABLE_FILE_SUFFIX = ".table.json";
  public static final String DATASYSTEM_FILE = "datasystem.json";

  public Optional<TableSource> readTableSource(TableSchema tableSchema, TableConfig tableConfig,
                                               ErrorCollector errors, NamePath basePath) {
    if (!tableConfig.getType().isSource()) {
      return Optional.empty();
    }
    //TableSource requires a schema
    if (tableSchema == null) {
      errors.warn("Found configuration for table [%s] but no schema. Table not loaded.",
          tableConfig.getResolvedName());
      return Optional.empty();
    }
    return Optional.of(tableConfig.initializeSource(errors, basePath, tableSchema));
  }

  public Optional<TableSink> readTableSink(TableSchema tableSchema, TableConfig tableConfig,
                                               ErrorCollector errors, NamePath basePath) {
    if (!tableConfig.getType().isSink()) {
      return Optional.empty();
    }
    return Optional.of(tableConfig.initializeSink(errors, basePath, Optional.of(tableSchema)));
  }
}