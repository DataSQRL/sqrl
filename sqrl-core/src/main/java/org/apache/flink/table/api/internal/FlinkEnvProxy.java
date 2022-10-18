package org.apache.flink.table.api.internal;

import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

/**
 * Lives in the flink package to access the protected createTable function
 */
public class FlinkEnvProxy {

  public static Table relNodeQuery(RelNode relNode, TableEnvironmentImpl environment) {
    PlannerQueryOperation plannerQueryOperation = new PlannerQueryOperation(relNode);
    return environment.createTable(plannerQueryOperation);
  }

  public static FunctionCatalog getFunctionCatalog(TableEnvironmentImpl environment) {
    StdTimeLibraryImpl.fncs.stream()
        .forEach(fn ->
            environment.functionCatalog.registerTemporarySystemFunction(fn.getName(),
            fn.getFnc(), true));
    return environment.functionCatalog;
  }
  public static FunctionCatalog getFunctionCatalog(StreamTableEnvironmentImpl environment) {
    StdTimeLibraryImpl.fncs.stream()
        .forEach(fn ->
            environment.functionCatalog.registerTemporarySystemFunction(fn.getName(),
            fn.getFnc(), true));
    return environment.functionCatalog;
  }
}
