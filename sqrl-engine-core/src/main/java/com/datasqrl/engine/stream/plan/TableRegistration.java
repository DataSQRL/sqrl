/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.plan;

import com.datasqrl.plan.calcite.table.ImportedRelationalTable;
import com.datasqrl.plan.calcite.table.StreamRelationalTable;

public interface TableRegistration<R, C> {

  public R accept(StreamRelationalTable table, C context);
  public R accept(ImportedRelationalTable table, C context);

  public interface TableRegistrationContext {

  }
}
