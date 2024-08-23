package com.datasqrl.engine.database.relational;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import lombok.Value;

@Value
public class IcebergPlan implements DatabasePhysicalPlan {

  List<SqlDDLStatement> ddl;

  Map<String, DatabasePhysicalPlan> engines;

  @JsonIgnore
  @Override
  public Map<IdentifiedQuery, QueryTemplate> getQueryPlans() {
    //Return first non-empty query plan from all query engines
    return engines.values().stream().map(DatabasePhysicalPlan::getQueryPlans).filter(Predicate.not(Map::isEmpty))
        .findFirst().orElse(Map.of());
  }

}
