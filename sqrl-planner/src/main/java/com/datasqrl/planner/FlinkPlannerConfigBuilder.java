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
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.logical.FlinkFilterProjectTransposeRule;
import scala.Tuple2;

@RequiredArgsConstructor
@Slf4j
public class FlinkPlannerConfigBuilder {

  /** Rules to remove from the logical program to disable filter pushdown in specific cases. */
  private static final List<? extends RelOptRule> FILTER_RULES_TO_REMOVE =
      List.of(
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          // ^ Removing prevents push filter through an aggregation
          FlinkFilterProjectTransposeRule.INSTANCE,
          // ^ Removing prevents push a filter past a project
          CoreRules.FILTER_SET_OP_TRANSPOSE
          // ^ Removing prevents cloning filters to each UNION/INTERSECT leg
          );

  private final CompilerConfig compilerConfig;
  private final SqrlFunctionCatalog sqrlFunctionCatalog;
  private final Configuration flinkConfig;

  @VisibleForTesting
  public FlinkPlannerConfigBuilder(CompilerConfig compilerConfig, Configuration flinkConfig) {
    this(compilerConfig, null, flinkConfig);
  }

  public PlannerConfig build() {
    var calciteConfigBuilder = new CalciteConfigBuilder();
    if (sqrlFunctionCatalog != null) {
      calciteConfigBuilder.addSqlOperatorTable(sqrlFunctionCatalog.getOperatorTable());
    }

    if (compilerConfig.predicatePushdownRules() != PredicatePushdownRules.DEFAULT) {
      var streamProgram = buildCustomStreamProgram(compilerConfig.predicatePushdownRules());
      calciteConfigBuilder.replaceStreamProgram(streamProgram);
    }

    return calciteConfigBuilder.build();
  }

  private FlinkChainedProgram<StreamOptimizeContext> buildCustomStreamProgram(
      PredicatePushdownRules rules) {

    var origStreamProgram = FlinkStreamProgram.buildProgram(flinkConfig);
    var customStreamProgram = new FlinkChainedProgram<StreamOptimizeContext>();

    for (var programName : origStreamProgram.getProgramNames()) {

      /*
       * Strip table scan related rules from PREDICATE_PUSHDOWN program.
       * This effectively means the following rules:
       * - PushPartitionIntoLegacyTableSourceScanRule
       * - PushPartitionIntoTableSourceScanRule
       * - PushFilterIntoTableSourceScanRule
       * - PushFilterIntoLegacyTableSourceScanRule
       */
      if (rules == PredicatePushdownRules.LIMITED_PP_RULES
          && programName.equals(FlinkStreamProgram.PREDICATE_PUSHDOWN())) {

        var program = origStreamProgram.get(programName).get();
        var changed =
            stripPrograms(
                program, elem -> elem._2 != null && elem._2.toLowerCase().contains("table scan"));

        if (!changed) {
          log.warn("Could not remove table scan related rules from PREDICATE_PUSHDOWN program");
        }
      }

      // Remove PREDICATE_PUSHDOWN program completely, and strip filter rules from the logical
      // program.
      if (rules == PredicatePushdownRules.LIMITED_FILTER_RULES) {

        if (programName.equals(FlinkStreamProgram.PREDICATE_PUSHDOWN())) {
          continue;
        }

        if (programName.equals(FlinkStreamProgram.LOGICAL())) {
          var logicalProgram = origStreamProgram.get(programName).get();
          stripRules(
              logicalProgram,
              r ->
                  FILTER_RULES_TO_REMOVE.stream()
                      .map(fr -> fr.getClass())
                      .anyMatch(cls -> cls.isInstance(r)));
        }
      }

      customStreamProgram.addLast(programName, origStreamProgram.get(programName).get());
    }

    return customStreamProgram;
  }

  // Strip program(s) from a FlinkGroupProgram instance based on a predicate.
  @SuppressWarnings({"unchecked"})
  private boolean stripPrograms(
      FlinkOptimizeProgram<?> flinkGroupProgram,
      Predicate<Tuple2<FlinkOptimizeProgram<?>, String>> shouldRemove) {
    try {
      var f = flinkGroupProgram.getClass().getDeclaredField("programs");
      f.setAccessible(true);

      var programs = (List<Tuple2<FlinkOptimizeProgram<?>, String>>) f.get(flinkGroupProgram);

      return programs.removeIf(shouldRemove);

    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to strip programs from FlinkGroupProgram", e);
    }
  }

  // Strip rule(s) from a FlinkOptimizeProgram instance based on a predicate.
  @SuppressWarnings("unchecked")
  private void stripRules(
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
