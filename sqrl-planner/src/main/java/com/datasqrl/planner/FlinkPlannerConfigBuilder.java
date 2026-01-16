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
import java.util.Set;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkGroupProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.logical.FlinkFilterProjectTransposeRule;
import org.apache.flink.table.planner.plan.rules.logical.FlinkProjectJoinTransposeRule;
import org.apache.flink.table.planner.plan.rules.logical.PushFilterInCalcIntoTableSourceScanRule;
import org.apache.flink.table.planner.plan.rules.logical.PushFilterIntoLegacyTableSourceScanRule;
import org.apache.flink.table.planner.plan.rules.logical.PushFilterIntoTableSourceScanRule;
import org.apache.flink.table.planner.plan.rules.logical.PushPartitionIntoLegacyTableSourceScanRule;
import org.apache.flink.table.planner.plan.rules.logical.PushPartitionIntoTableSourceScanRule;
import org.apache.flink.table.planner.plan.rules.logical.PushProjectIntoLegacyTableSourceScanRule;
import org.apache.flink.table.planner.plan.rules.logical.PushProjectIntoTableSourceScanRule;
import scala.Tuple2;

@RequiredArgsConstructor
@Slf4j
public class FlinkPlannerConfigBuilder {

  /** We do not touch these programs. */
  private static final Set<String> IGNORED_PROGRAMS =
      Set.of(
          FlinkStreamProgram.DECORRELATE(),
          FlinkStreamProgram.PHYSICAL_REWRITE(),
          FlinkStreamProgram.TIME_INDICATOR());

  /** Rules to remove in case of {@link PredicatePushdownRules#LIMITED_TABLE_SOURCE_RULES}. */
  private static final List<? extends RelOptRule> TABLE_SOURCE_RULES_TO_REMOVE =
      List.of(
          PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
          PushFilterIntoTableSourceScanRule.INSTANCE,
          PushPartitionIntoLegacyTableSourceScanRule.INSTANCE(),
          PushPartitionIntoTableSourceScanRule.INSTANCE,
          PushProjectIntoLegacyTableSourceScanRule.INSTANCE(),
          PushProjectIntoTableSourceScanRule.INSTANCE);

  /** Extra rules to remove in case of {@link PredicatePushdownRules#LIMITED_RULES}. */
  private static final List<? extends RelOptRule> GENERAL_FILTER_RULES_TO_REMOVE =
      List.of(
          // Removing prevents push filter through an aggregation
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          // Removing prevents cloning filters to each UNION/INTERSECT leg
          CoreRules.FILTER_SET_OP_TRANSPOSE,
          // Removing CoreRules.FILTER_PROJECT_TRANSPOSE keeps filters above projects (no rewrite
          // like a+1>10→a>9), reducing predicate pushdown/pruning but often improving subgraph
          // reuse by avoiding per-branch filter clones.
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          // Removing keeps WHERE conditions above projections (no time-aware rewrite), limiting
          // pushdown and simplification but avoiding duplicated Calcs across branches—useful for
          // subgraph elimination.
          FlinkFilterProjectTransposeRule.INSTANCE,
          // Removing FlinkProjectJoinTransposeRule stops pushing projects into each join input,
          // leading to wider joins (less column pruning) but fewer per-arm Calcs and more identical
          // subgraphs around (temporal) joins.
          FlinkProjectJoinTransposeRule.INSTANCE);

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
      var program = origStreamProgram.get(programName).get();

      // Programs that can be ignored.
      if (IGNORED_PROGRAMS.contains(programName)) {
        customStreamProgram.addLast(programName, program);
        continue;
      }

      if (rules == PredicatePushdownRules.LIMITED_TABLE_SOURCE_RULES) {
        removeTableSourceScanRules(programName, program);
      }

      if (rules == PredicatePushdownRules.LIMITED_RULES) {
        removeTableSourceScanRules(programName, program);
        stripRules(programName, program, r -> anyMatch(GENERAL_FILTER_RULES_TO_REMOVE, r));
      }

      customStreamProgram.addLast(programName, program);
    }

    return customStreamProgram;
  }

  private void removeTableSourceScanRules(
      String programName, FlinkOptimizeProgram<StreamOptimizeContext> program) {

    if (programName.equals(FlinkStreamProgram.PREDICATE_PUSHDOWN())) {
      var changed =
          stripPrograms(
              program, elem -> elem._2 != null && elem._2.toLowerCase().contains("table scan"));

      if (!changed) {
        log.warn("Could not remove table scan related rules from PREDICATE_PUSHDOWN program");
      }
    }

    stripRules(programName, program, r -> anyMatch(TABLE_SOURCE_RULES_TO_REMOVE, r));
    stripRules(
        programName, program, r -> matches(PushFilterInCalcIntoTableSourceScanRule.INSTANCE, r));
  }

  private <T extends RelOptRule> boolean matches(T ruleToMatch, RelOptRule rule) {
    return anyMatch(List.of(ruleToMatch), rule);
  }

  private boolean anyMatch(List<? extends RelOptRule> rulesToMatch, RelOptRule rule) {
    return rulesToMatch.stream().map(fr -> fr.getClass()).anyMatch(cls -> cls.isInstance(rule));
  }

  ////////////////////////////////////////////////////////////////////////////////
  ///// Reflection utils to access private Flink class fields
  ////////////////////////////////////////////////////////////////////////////////

  // Strip program(s) from a FlinkGroupProgram instance based on a predicate.
  private boolean stripPrograms(
      FlinkOptimizeProgram<?> flinkGroupProgram,
      Predicate<Tuple2<FlinkOptimizeProgram<?>, String>> shouldRemove) {

    var programs = extractGroupPrograms(flinkGroupProgram);

    return programs.removeIf(shouldRemove);
  }

  // Strip rule(s) from a FlinkOptimizeProgram instance based on a predicate.
  // In case of a FlinkGroupProgram, rule strip happens for every program.
  @SuppressWarnings("unchecked")
  private void stripRules(
      String programName, FlinkOptimizeProgram<?> program, Predicate<RelOptRule> shouldRemove) {

    List<?> programs;
    if (program instanceof FlinkGroupProgram) {
      programs = extractGroupPrograms(program).stream().map(t -> t._1).toList();

    } else {
      programs = List.of(program);
    }

    for (var internalProgram : programs) {
      if (internalProgram instanceof FlinkGroupProgram) {
        stripRules(programName, (FlinkOptimizeProgram<?>) internalProgram, shouldRemove);
        continue;
      }

      try {
        var f = internalProgram.getClass().getSuperclass().getDeclaredField("rules");
        f.setAccessible(true);

        var current = (List<RelOptRule>) f.get(internalProgram);
        var mutable = new ArrayList<>(current);

        var changed = mutable.removeIf(shouldRemove);
        if (changed) {
          f.set(internalProgram, List.copyOf(mutable));
        }
      } catch (ReflectiveOperationException e) {
        log.warn("Could not strip rules from program: " + programName, e);
      }
    }
  }

  // Extract the internal program list of a FlinkGroupProgram.
  @SuppressWarnings("unchecked")
  private List<Tuple2<FlinkOptimizeProgram<?>, String>> extractGroupPrograms(
      FlinkOptimizeProgram<?> groupProgram) {

    if (groupProgram instanceof FlinkGroupProgram) {
      try {
        var f = groupProgram.getClass().getDeclaredField("programs");
        f.setAccessible(true);

        return (List<Tuple2<FlinkOptimizeProgram<?>, String>>) f.get(groupProgram);

      } catch (ReflectiveOperationException e) {
        log.warn("Could not extract internal program list of a FlinkGroupProgram", e);
      }
    } else {
      log.warn("Expected a FlinkGroupProgram, got: {}", groupProgram.getClass());
    }

    return List.of();
  }
}
