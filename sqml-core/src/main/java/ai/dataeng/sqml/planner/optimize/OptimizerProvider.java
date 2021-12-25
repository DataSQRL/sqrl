package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.execution.Optimizer;

public interface OptimizerProvider {
  Optimizer createOptimizer();
}
