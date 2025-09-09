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
package com.datasqrl.planner;

import com.datasqrl.config.PackageJson.CompilerConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.logical.FlinkFilterJoinRule;
import org.apache.flink.table.planner.plan.rules.logical.FlinkFilterProjectTransposeRule;

@RequiredArgsConstructor
public class FlinkPlannerConfigBuilder {

  /** Rules to remove from the logical program to disable filter pushdown. */
  private static final List<Class<? extends RelOptRule>> FILTER_RULES_TO_REMOVE =
      List.of(
          FlinkFilterJoinRule.FILTER_INTO_JOIN.getClass(),
          // push filter into the children of a join
          FlinkFilterJoinRule.JOIN_CONDITION_PUSH.getClass(),
          // push filter through an aggregation
          CoreRules.FILTER_AGGREGATE_TRANSPOSE.getClass(),
          // push a filter past a project
          FlinkFilterProjectTransposeRule.INSTANCE.getClass(),
          // push a filter past a setop
          CoreRules.FILTER_SET_OP_TRANSPOSE.getClass());

  private final CompilerConfig compilerConfig;
  private final SqrlFunctionCatalog sqrlFunctionCatalog;
  private final Configuration flinkConfig;

  public PlannerConfig build() {
    var calciteConfigBuilder = new CalciteConfigBuilder();
    calciteConfigBuilder.addSqlOperatorTable(sqrlFunctionCatalog.getOperatorTable());

    if (compilerConfig.disablePredicatePushdown()) {
      var sqrlStreamProgram = buildNoPredicatePushdownStreamProgram();
      calciteConfigBuilder.replaceStreamProgram(sqrlStreamProgram);
    }

    return calciteConfigBuilder.build();
  }

  private FlinkChainedProgram<StreamOptimizeContext> buildNoPredicatePushdownStreamProgram() {
    var origFlinkStreamProgram = FlinkStreamProgram.buildProgram(flinkConfig);
    var sqrlStreamProgram = new FlinkChainedProgram<StreamOptimizeContext>();

    for (var programName : origFlinkStreamProgram.getProgramNames()) {
      // Omit predicate pushdown program completely
      if (!programName.equals(FlinkStreamProgram.PREDICATE_PUSHDOWN())) {
        sqrlStreamProgram.addLast(programName, origFlinkStreamProgram.get(programName).get());
      }

      // Remove specific filter rules from the logical program
      if (programName.equals(FlinkStreamProgram.LOGICAL())) {
        var logicalProgram = origFlinkStreamProgram.get(programName).get();
        stripRules(
            logicalProgram,
            r -> FILTER_RULES_TO_REMOVE.stream().anyMatch(rule -> rule.isInstance(r)));

        sqrlStreamProgram.addLast(programName, logicalProgram);
      }
    }

    return sqrlStreamProgram;
  }

  /**
   * Strip rules from a FlinkOptimizeProgram instance based on a predicate. Has to be called BEFORE
   * the program is executed by the optimizer.
   */
  @SuppressWarnings("unchecked")
  private static void stripRules(
      FlinkOptimizeProgram<?> flinkRuleSetProgram, Predicate<RelOptRule> shouldRemove) {
    try {
      var f = flinkRuleSetProgram.getClass().getSuperclass().getDeclaredField("rules");
      f.setAccessible(true);

      var current = (List<RelOptRule>) f.get(flinkRuleSetProgram);
      var mutable = new ArrayList<>(current);

      var changed = mutable.removeIf(shouldRemove);
      if (changed) {
        f.set(flinkRuleSetProgram, List.copyOf(mutable));
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to strip rules from FlinkRuleSetProgram", e);
    }
  }
}
