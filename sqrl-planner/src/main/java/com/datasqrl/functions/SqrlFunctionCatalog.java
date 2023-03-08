package com.datasqrl.functions;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.flink.table.functions.UserDefinedFunction;

public interface SqrlFunctionCatalog {

  SqlOperatorTable getOperatorTable();

  void addNativeFunction(String name, UserDefinedFunction function);
}
