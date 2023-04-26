/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.formats.FormatFactory;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class TableSink extends AbstractExternalTable {

  private final Optional<TableSchema> schema;

  public TableSink(@NonNull DataSystemConnectorSettings connector,
      @NonNull TableConfig configuration,
      @NonNull NamePath path, @NonNull Name name,
      Optional<TableSchema> schema) {
    super(connector, configuration, path, name);
    this.schema = schema;
  }

  public FormatFactory.Writer getWriter() {
    FormatFactory format = configuration.getFormat();
    return format.getWriter(configuration.getFormatConfig());
  }


}
