/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.io.tables.TableSource;

public interface ImportedRelationalTable extends SourceRelationalTable {

  TableSource getTableSource();
  RelDataType getRowType();
}
