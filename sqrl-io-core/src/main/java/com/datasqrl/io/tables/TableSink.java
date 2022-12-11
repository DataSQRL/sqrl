/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import lombok.Getter;
import lombok.NonNull;

import java.util.Optional;

@Getter
public class TableSink extends AbstractExternalTable {

  private final Optional<TableSchema> schema;


  public TableSink(@NonNull DataSystemConnector dataset, @NonNull TableConfig configuration,
      @NonNull NamePath path, @NonNull Name name,
      Optional<TableSchema> schema) {
    super(dataset, configuration, path, name);
    this.schema = schema;
  }

  public Format.Writer getWriter() {
    FormatConfiguration formatConfig = configuration.getFormat();
    return formatConfig.getImplementation().getWriter(formatConfig);
  }


}
