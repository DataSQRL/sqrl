package com.datasqrl.engine;

import com.datasqrl.util.StreamUtil;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.queries.APIQuery;
import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
public class PhysicalPlan {

  List<StagePlan> stagePlans;

  public Map<APIQuery, QueryTemplate> getDatabaseQueries() {
    return getPlans(DatabasePhysicalPlan.class).flatMap(dbPlan -> dbPlan.getQueries().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
  }

  public<T extends EnginePhysicalPlan> Stream<T> getPlans(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(StagePlan::getPlan), clazz);
  }

  @Value
  public static class StagePlan {

    ExecutionStage stage;
    EnginePhysicalPlan plan;

  }


}
