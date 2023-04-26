/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.SchemaValidator;

public interface TableSchema {
  RowMapper getRowMapper(RowConstructor rowConstructor,
      DataSystemConnectorSettings connectorSettings);

  Name getTableName();

  String getSchemaType();

  SchemaValidator getValidator(SchemaAdjustmentSettings settings,
      DataSystemConnectorSettings connectorSettings);

  String getDefinition();

}
