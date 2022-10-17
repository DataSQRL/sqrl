package org.apache.flink.table.api.internal;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.Table;
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
    return environment.functionCatalog;
  }
}
