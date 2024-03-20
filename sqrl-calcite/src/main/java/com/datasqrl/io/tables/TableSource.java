/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;
import lombok.Getter;

/**
 * A {@link TableSource} defines an input source to be imported into an SQRL script. A
 * {@link TableSource} is comprised of records and is the smallest unit of data that one can refer
 * to within an SQRL script.
 */
@Getter
public class TableSource extends TableInput {

  private final TableSchema schema;

  public TableSource(TableConfig configuration, NamePath path,
      Name name, TableSchema schema) {
    super(configuration, path, name, Optional.ofNullable(schema));
    this.schema = schema;
  }
}
