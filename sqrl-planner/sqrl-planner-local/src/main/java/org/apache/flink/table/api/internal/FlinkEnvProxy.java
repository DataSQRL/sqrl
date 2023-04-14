package org.apache.flink.table.api.internal;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

/**
 * Lives in the flink package to access the protected createTable function
 */
public class FlinkEnvProxy {
  public static SqlOperatorTable getOperatorTable(TableEnvironmentImpl env) {
    FunctionCatalog catalog = env.functionCatalog;

    SqlOperatorTable operatorTable = SqlOperatorTables.chain(
        new FunctionCatalogOperatorTable(
            catalog,
            env.getCatalogManager().getDataTypeFactory(),
            new FlinkTypeFactory(env.getClass().getClassLoader(), FlinkTypeSystem.INSTANCE),
            null),
        FlinkSqlOperatorTable.instance()
    );
    return operatorTable;
  }
}
