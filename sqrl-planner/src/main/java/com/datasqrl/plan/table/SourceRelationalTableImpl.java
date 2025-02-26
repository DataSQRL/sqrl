/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import lombok.NonNull;

/**
 * An abstract source table that represents the source of table data.
 *
 * <p>This class is wrapped by a {@link ProxyImportRelationalTable} to be used in Calcite schema
 */
public abstract class SourceRelationalTableImpl extends AbstractRelationalTable
    implements SourceRelationalTable {

  protected SourceRelationalTableImpl(@NonNull Name nameId) {
    super(nameId);
  }
}
