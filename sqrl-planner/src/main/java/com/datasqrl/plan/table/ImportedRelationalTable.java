/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.io.tables.TableSource;
import org.apache.calcite.rel.type.RelDataType;

public interface ImportedRelationalTable extends SourceRelationalTable {

  TableSource getTableSource();

  RelDataType getRowType();
}
