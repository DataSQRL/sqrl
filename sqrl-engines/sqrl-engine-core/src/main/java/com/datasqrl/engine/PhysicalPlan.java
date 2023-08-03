/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.queries.APIQuery;
import java.io.IOException;
import java.nio.file.Path;
import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
public class PhysicalPlan {

  List<StagePlan> stagePlans;

  public Map<IdentifiedQuery, QueryTemplate> getDatabaseQueries() {
    return getPlans(DatabasePhysicalPlan.class).flatMap(
            dbPlan -> dbPlan.getQueries().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public <T extends EnginePhysicalPlan> Stream<T> getPlans(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(StagePlan::getPlan), clazz);
  }

  public void writeTo(Path deployDir, Deserializer serializer) throws IOException {
    for (StagePlan stagePlan : stagePlans) {
      stagePlan.plan.writeTo(deployDir, stagePlan.stage.getName(), serializer);
    }
  }

  @Value
  public static class StagePlan {

    ExecutionStage stage;
    EnginePhysicalPlan plan;

  }


}
