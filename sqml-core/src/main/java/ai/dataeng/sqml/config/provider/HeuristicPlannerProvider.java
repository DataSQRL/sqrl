package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.planner.Planner;

public interface HeuristicPlannerProvider {

  Planner createPlanner();
}
