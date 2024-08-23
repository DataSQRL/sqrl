package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.plan.queries.IdentifiedQuery;
import java.util.Map;

public class DuckDBPhysicalPlan implements DatabasePhysicalPlan {

  @Override
  public Map<IdentifiedQuery, QueryTemplate> getQueryPlans() {
    return Map.of();
  }
}
