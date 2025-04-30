/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import java.util.Optional;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;

import lombok.Getter;

/**
 * A {@link TableSource} defines an input source to be imported into an SQRL script. A
 * {@link TableSource} is comprised of records and is the smallest unit of data that one can refer
 * to within an SQRL script.
 */
@Getter
public class TableSource extends TableInput {

  public TableSource(TableConfig configuration, NamePath path,
      Name name, TableSchema schema) {
    super(configuration, path, name, Optional.ofNullable(schema));
  }

  public static TableSource create(TableConfig tableConfig, NamePath basePath, TableSchema schema) {
//    getErrors().checkFatal(getBase().getType().isSource(), "Table is not a source: %s", name);
    var tableName = tableConfig.getName();
    return new TableSource(tableConfig, basePath.concat(tableName), tableName, schema);
  }
}
