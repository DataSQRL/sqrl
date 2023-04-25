/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.schema.input.SchemaValidator;

public class TableInput extends AbstractExternalTable {

  public TableInput(DataSystemConnector dataset, TableConfig configuration, NamePath path,
      Name name) {
    super(dataset, configuration, path, name);
  }

  public boolean hasSourceTimestamp() {
    return connector.hasSourceTimestamp();
  }

  public FormatFactory.Parser getParser() {
    FormatFactory format = configuration.getFormat();
    return format.getParser(configuration.getFormatConfig());
  }

  public SchemaValidator getSchemaValidator() {
    return null;
  }
}