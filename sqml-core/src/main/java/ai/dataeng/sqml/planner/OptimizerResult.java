package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Schema;
import ai.dataeng.sqml.execution.ExecutionPlan;
import lombok.Value;

@Value
public class OptimizerResult {
  LogicalPlan logicalPlan;
  ExecutionPlan executionPlan;
}
