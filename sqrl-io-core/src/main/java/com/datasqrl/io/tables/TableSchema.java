/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.name.Name;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.SchemaValidator;

public interface TableSchema {
  RowMapper getRowMapper(RowConstructor rowConstructor,
      boolean hasSourceTimestamp);

  Name getName();

  SchemaValidator getValidator(TableConfig config, boolean hasSourceTimestamp);

  UniversalTable createUniversalTable(boolean hasSourceTimestamp);
}
