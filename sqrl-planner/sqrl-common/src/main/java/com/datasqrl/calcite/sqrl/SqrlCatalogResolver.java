package com.datasqrl.calcite.sqrl;

import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public interface SqrlCatalogResolver {
  RelOptTable getTableFromPath(List<String> names);
  SqlUserDefinedTableFunction getTableFunction(List<String> path);
}
