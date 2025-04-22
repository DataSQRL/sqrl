package com.datasqrl.plan.local.generate;

import org.apache.calcite.schema.TableFunction;

import com.datasqrl.plan.table.QueryRelationalTable;

public interface QueryTableFunction extends TableFunction {

    QueryRelationalTable getQueryTable();

}
