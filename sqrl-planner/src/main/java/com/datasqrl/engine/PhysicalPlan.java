/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.engine.database.DatabasePhysicalPlanOld;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

@Value
public class PhysicalPlan {

  List<StagePlan> stagePlans;

  public Map<IdentifiedQuery, QueryTemplate> getDatabaseQueries() {
    return getPlans(DatabasePhysicalPlanOld.class).flatMap(
            dbPlan -> dbPlan.getQueryPlans().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public <T extends EnginePhysicalPlan> Stream<T> getPlans(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(StagePlan::getPlan), clazz);
  }

  @Value
  public static class StagePlan {

    ExecutionStage stage;
    EnginePhysicalPlan plan;

  }


}
