/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.tables.TableSource;

public interface ImportedRelationalTable extends SourceRelationalTable {

  TableSource getTableSource();

}
