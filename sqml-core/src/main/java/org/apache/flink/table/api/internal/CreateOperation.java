package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

public class CreateOperation {

  public static TableImpl create(StreamTableEnvironmentImpl tEnv,
      QueryOperation plannerQueryOperation) {
    return tEnv.createTable(plannerQueryOperation);
  }
}
