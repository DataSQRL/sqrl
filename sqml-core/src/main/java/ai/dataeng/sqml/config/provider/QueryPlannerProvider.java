package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.planner.QueryPlanner;

public interface QueryPlannerProvider {
  QueryPlanner getQueryPlanner();
}
