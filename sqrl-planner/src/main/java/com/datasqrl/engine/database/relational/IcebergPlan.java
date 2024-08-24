package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class IcebergPlan implements DatabasePhysicalPlan {

  DatabasePhysicalPlan plan;

  @JsonIgnore
  Map<IdentifiedQuery, QueryTemplate> queryPlans;

  @JsonIgnore
  @Override
  public List<SqlDDLStatement> getDdl() {
    return plan.getDdl();
  }
}
