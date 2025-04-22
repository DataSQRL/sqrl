/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datasqrl.engine.database.DatabasePhysicalPlanOld;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.PhysicalPlanRewriter;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class PhysicalPlan {

  @Singular
  List<PhysicalStagePlan> stagePlans;

  public Map<IdentifiedQuery, QueryTemplate> getDatabaseQueries() {
    return getPlans(DatabasePhysicalPlanOld.class).flatMap(
            dbPlan -> dbPlan.getQueryPlans().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public <T extends EnginePhysicalPlan> Stream<T> getPlans(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(PhysicalStagePlan::getPlan), clazz);
  }

  public PhysicalPlan applyRewriting(Collection<PhysicalPlanRewriter> rewriters, Sqrl2FlinkSQLTranslator sqrlEnv) {
    if (rewriters.isEmpty()) {
		return this;
	}
    var builder = PhysicalPlan.builder();
    for (PhysicalStagePlan stagePlan : stagePlans) {
      var enginePlan = stagePlan.plan;
      for (PhysicalPlanRewriter rewriter : rewriters) {
        if (rewriter.appliesTo(enginePlan)) {
          enginePlan = rewriter.rewrite(enginePlan, sqrlEnv);
        }
      }
      builder.stagePlan(new PhysicalStagePlan(stagePlan.stage, enginePlan));
    }
    return builder.build();
  }

  @Value
  public static class PhysicalStagePlan {

    ExecutionStage stage;
    EnginePhysicalPlan plan;

  }


}
