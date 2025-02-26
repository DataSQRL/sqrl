/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import java.util.Optional;
import lombok.Getter;

@Getter
public class TableInput extends AbstractExternalTable {

  public TableInput(
      TableConfig configuration, NamePath path, Name name, Optional<TableSchema> tableSchema) {
    super(configuration, path, name, tableSchema);
  }
}
