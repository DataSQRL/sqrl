package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

public class AddPlanToEnv {

  public static Table addPlanToEnv(PlannerQueryOperation operation, TableEnvironmentImpl environment) {
    return environment.createTable(operation);
  }
}
