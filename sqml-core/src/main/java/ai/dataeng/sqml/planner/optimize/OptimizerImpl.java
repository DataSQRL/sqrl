package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.execution.Optimizer;
import ai.dataeng.sqml.planner.OptimizerResult;
import ai.dataeng.sqml.planner.PlannerResult;

public class OptimizerImpl implements Optimizer {

  @Override
  public OptimizerResult optimize(PlannerResult result) {
    return new OptimizerResult(null, null);
  }
}
