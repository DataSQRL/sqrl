/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.PhysicalPlanRewriter;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.util.StreamUtil;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class PhysicalPlan {

  @Singular List<PhysicalStagePlan> stagePlans;

  public <T extends EnginePhysicalPlan> Stream<T> getPlans(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(PhysicalStagePlan::plan), clazz);
  }

  public PhysicalPlan applyRewriting(
      Collection<PhysicalPlanRewriter> rewriters, Sqrl2FlinkSQLTranslator sqrlEnv) {
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

  public record PhysicalStagePlan(ExecutionStage stage, EnginePhysicalPlan plan) {}
}
