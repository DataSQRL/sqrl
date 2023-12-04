/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.engine.stream.RowMapper;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import java.net.URI;
import java.util.Optional;

public interface TableSchema {

  RowMapper getRowMapper(RowConstructor rowConstructor,
      DataSystemConnectorSettings connectorSettings);

  String getSchemaType();

  SchemaValidator getValidator(SchemaAdjustmentSettings settings,
      DataSystemConnectorSettings connectorSettings);

  String getDefinition();

  Optional<URI> getLocation();

}
