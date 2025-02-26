package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.table.QueryRelationalTable;
import org.apache.calcite.schema.TableFunction;

public interface QueryTableFunction extends TableFunction {

  QueryRelationalTable getQueryTable();
}
