/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.*;
import com.datasqrl.canonicalizer.NamePath;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class DataSource {

  public static final String TABLE_FILE_SUFFIX = ".table.sql";
  public static final String DATASYSTEM_FILE_PREFIX = "dynamic.sink";

  public Optional<TableSource> readTableSource(TableSchema tableSchema, TableConfig tableConfig,
                                               ErrorCollector errors, NamePath basePath) {
    if (!tableConfig.getBase().getType().isSource()) {
      return Optional.empty();
    }
    //TableSource requires a schema
    if (tableSchema == null) {
      errors.warn("Found configuration for table [%s] but no schema. Table not loaded.",
          tableConfig.getName());
      return Optional.empty();
    }
    return Optional.of(TableSource.create(tableConfig, basePath, tableSchema));
  }

  public Optional<TableSink> readTableSink(Optional<TableSchema> schema, TableConfig tableConfig, NamePath basePath) {
    if (!tableConfig.getBase().getType().isSink()) {
      return Optional.empty();
    }

    return Optional.of(TableSinkImpl.create(tableConfig, basePath, schema));
  }
}