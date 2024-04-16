/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.log.kafka.KafkaPhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
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
    return getPlans(DatabasePhysicalPlan.class).flatMap(
            dbPlan -> dbPlan.getQueries().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public <T extends EnginePhysicalPlan> Stream<T> getPlans(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(StagePlan::getPlan), clazz);
  }

  public EnginePhysicalPlan get(String name) {
    switch (name.toLowerCase()) {
      case "log": return getPlans(KafkaPhysicalPlan.class).findFirst().get();
      case "database": return getPlans(JDBCPhysicalPlan.class).findFirst().get();
      case "server": return getPlans(ServerPhysicalPlan.class).findFirst().get();
      case "streams": return getPlans(FlinkStreamPhysicalPlan.class).findFirst().get();
      default:
        throw new RuntimeException("Could not find plan");
    }
  }

  @Value
  public static class StagePlan {

    ExecutionStage stage;
    EnginePhysicalPlan plan;

  }


}
