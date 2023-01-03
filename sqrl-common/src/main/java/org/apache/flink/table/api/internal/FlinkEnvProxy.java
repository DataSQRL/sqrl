package org.apache.flink.table.api.internal;

import com.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import com.datasqrl.function.builtin.time.FlinkFnc;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

/**
 * Lives in the flink package to access the protected createTable function
 */
public class FlinkEnvProxy {

  public static Table relNodeQuery(RelNode relNode, TableEnvironmentImpl environment) {
    PlannerQueryOperation plannerQueryOperation = new PlannerQueryOperation(relNode);
    plannerQueryOperation.getResolvedSchema();
    return environment.createTable(plannerQueryOperation);
  }

  public static FunctionCatalog getFunctionCatalog(TableEnvironmentImpl environment) {
    StdTimeLibraryImpl.fncs.stream()
        .forEach(fn ->
            environment.functionCatalog.registerTemporarySystemFunction(fn.getName(),
                fn.getFnc(), true));
    return environment.functionCatalog;
  }
  //todo fix
  static TableEnvironmentImpl t = TableEnvironmentImpl.create(
      EnvironmentSettings.inStreamingMode().getConfiguration()
  );
  public static SqlOperatorTable getOperatorTable(List<FlinkFnc> envFunctions) {


    FunctionCatalog catalog = FlinkEnvProxy.getFunctionCatalog(t);

    envFunctions.forEach(fn -> catalog.registerTemporarySystemFunction(fn.getName(),
        fn.getFnc(), true));

    SqlOperatorTable operatorTable = SqlOperatorTables.chain(
        new FunctionCatalogOperatorTable(
            catalog,
            t.getCatalogManager().getDataTypeFactory(),
            new FlinkTypeFactory(new FlinkTypeSystem())),
        FlinkSqlOperatorTable.instance()
    );
    return operatorTable;
  }
}
