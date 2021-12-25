package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.planner.OptimizerResult;
import ai.dataeng.sqml.planner.PlannerResult;

public interface Optimizer {

  OptimizerResult optimize(PlannerResult result);
}
