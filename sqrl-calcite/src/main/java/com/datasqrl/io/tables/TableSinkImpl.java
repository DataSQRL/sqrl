/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class TableSinkImpl extends AbstractExternalTable implements TableSink {

  public TableSinkImpl(
      @NonNull TableConfig configuration,
      @NonNull NamePath path, @NonNull Name name,
      Optional<TableSchema> schema) {
    super(configuration, path, name, schema);
  }

}
