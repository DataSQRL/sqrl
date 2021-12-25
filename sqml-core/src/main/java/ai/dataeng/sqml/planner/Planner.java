package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Namespace;

public interface Planner {

  PlannerResult plan(Namespace namespace);
}
