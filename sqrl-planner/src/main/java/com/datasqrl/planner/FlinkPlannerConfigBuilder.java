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
import com.datasqrl.engine.stream.flink.sql.rules.SqrlCalcMergeRule;
import com.datasqrl.engine.stream.flink.sql.rules.SqrlMiniBatchIntervalInferRule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkGroupProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkRuleSetProgram;
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
import org.apache.flink.table.planner.plan.rules.physical.stream.MiniBatchIntervalInferRule;
import scala.Tuple2;

@RequiredArgsConstructor
@Slf4j
public class FlinkPlannerConfigBuilder {

  /** We do not strip rules from these programs. */
  private static final Set<String> IGNORED_PROGRAMS =
      Set.of(FlinkStreamProgram.DECORRELATE(), FlinkStreamProgram.TIME_INDICATOR());

  /**
   * Downstream filter rules to remove in case of {@link
   * PredicatePushdownRules#LIMITED_RULES_NO_SOURCE}.
   */
  private static final List<RelOptRule> BASE_FILTER_RULES_TO_REMOVE =
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
          FlinkFilterProjectTransposeRule.INSTANCE);

  /** Downstream rules to remove in case of {@link PredicatePushdownRules#LIMITED_RULES} */
  private static final List<RelOptRule> EXTENDED_FILTER_RULES_TO_REMOVE =
      ImmutableList.<RelOptRule>builder()
          .addAll(BASE_FILTER_RULES_TO_REMOVE)
          .add(
              // Removing FlinkProjectJoinTransposeRule stops pushing projects into each join input,
              // leading to wider joins (less column pruning) but fewer per-arm Calcs and more
              // identical
              // subgraphs around (temporal) joins.
              FlinkProjectJoinTransposeRule.INSTANCE)
          .build();

  /** Table source rules to remove in case of {@link PredicatePushdownRules#LIMITED_RULES}. */
  private static final List<RelOptRule> TABLE_SOURCE_RULES_TO_REMOVE =
      List.of(
          PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
          PushFilterIntoTableSourceScanRule.INSTANCE,
          PushFilterInCalcIntoTableSourceScanRule.INSTANCE,
          PushPartitionIntoLegacyTableSourceScanRule.INSTANCE(),
          PushPartitionIntoTableSourceScanRule.INSTANCE,
          PushProjectIntoLegacyTableSourceScanRule.INSTANCE(),
          PushProjectIntoTableSourceScanRule.INSTANCE);

  private final CompilerConfig compilerConfig;
  private final SqrlFunctionCatalog sqrlFunctionCatalog;
  private final Configuration flinkConfig;
  private final FlinkOptimizeProgram<StreamOptimizeContext> insertConflictProgram;

  @VisibleForTesting
  public FlinkPlannerConfigBuilder(CompilerConfig compilerConfig, Configuration flinkConfig) {
    this(compilerConfig, null, flinkConfig, null);
  }

  public PlannerConfig build() {
    var calciteConfigBuilder = new CalciteConfigBuilder();
    if (sqrlFunctionCatalog != null) {
      calciteConfigBuilder.addSqlOperatorTable(sqrlFunctionCatalog.getOperatorTable());
    }

    var streamProgram = buildCustomStreamProgram(compilerConfig.predicatePushdownRules());
    calciteConfigBuilder.replaceStreamProgram(streamProgram);

    return calciteConfigBuilder.build();
  }

  private FlinkChainedProgram<StreamOptimizeContext> buildCustomStreamProgram(
      PredicatePushdownRules rules) {

    var origStreamProgram = FlinkStreamProgram.buildProgram(flinkConfig);
    var customStreamProgram = new FlinkChainedProgram<StreamOptimizeContext>();

    for (var programName : origStreamProgram.getProgramNames()) {
      var program = origStreamProgram.get(programName).get();

      if (programName.equals(FlinkStreamProgram.PHYSICAL_REWRITE())
          && insertConflictProgram != null) {
        addAfterChangelogModeInference(program, insertConflictProgram);
      }

      // Programs that can be ignored.
      if (IGNORED_PROGRAMS.contains(programName)) {
        customStreamProgram.addLast(programName, program);
        continue;
      }

      replaceRules(programName, program, SqrlCalcMergeRule::replacing);

      if (programName.equals(FlinkStreamProgram.PHYSICAL_REWRITE())) {
        replaceRules(
            programName,
            program,
            rule ->
                rule instanceof MiniBatchIntervalInferRule
                    ? SqrlMiniBatchIntervalInferRule.INSTANCE
                    : rule);
        customStreamProgram.addLast(programName, program);
        continue;
      }

      if (rules == PredicatePushdownRules.LIMITED_TABLE_SOURCE_RULES) {
        removeTableSourceScanRules(programName, program);
      }

      if (rules == PredicatePushdownRules.LIMITED_RULES_NO_SOURCE) {
        stripRules(programName, program, r -> anyMatch(BASE_FILTER_RULES_TO_REMOVE, r));
      }

      if (rules == PredicatePushdownRules.LIMITED_RULES) {
        removeTableSourceScanRules(programName, program);
        stripRules(programName, program, r -> anyMatch(EXTENDED_FILTER_RULES_TO_REMOVE, r));
      }

      customStreamProgram.addLast(programName, program);
    }

    return customStreamProgram;
  }

  private void addAfterChangelogModeInference(
      FlinkOptimizeProgram<StreamOptimizeContext> physicalRewrite,
      FlinkOptimizeProgram<StreamOptimizeContext> program) {

    var programs = extractGroupPrograms(physicalRewrite);
    for (int i = 0; i < programs.size(); i++) {
      if (programs.get(i)._1 instanceof FlinkChangelogModeInferenceProgram) {
        programs.add(i + 1, new Tuple2<>(program, "insert conflict resolution"));
        return;
      }
    }

    throw new IllegalStateException("Stream program has no changelog mode inference");
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
  }

  private boolean anyMatch(List<RelOptRule> rulesToMatch, RelOptRule rule) {
    return rulesToMatch.stream().map(RelOptRule::getClass).anyMatch(cls -> cls.isInstance(rule));
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

  private void stripRules(
      String programName, FlinkOptimizeProgram<?> program, Predicate<RelOptRule> shouldRemove) {
    rewriteRules(programName, program, rules -> rules.removeIf(shouldRemove));
  }

  private void replaceRules(
      String programName, FlinkOptimizeProgram<?> program, UnaryOperator<RelOptRule> replacement) {
    rewriteRules(programName, program, rules -> rules.replaceAll(replacement));
  }

  // Rewrite the rule list of a FlinkOptimizeProgram instance.
  // In case of a FlinkGroupProgram, the rewrite happens for every program.
  @SuppressWarnings("unchecked")
  private void rewriteRules(
      String programName, FlinkOptimizeProgram<?> program, Consumer<List<RelOptRule>> rewrite) {

    List<?> programs;
    if (program instanceof FlinkGroupProgram) {
      programs = extractGroupPrograms(program).stream().map(t -> t._1).toList();

    } else {
      programs = List.of(program);
    }

    for (var internalProgram : programs) {
      if (internalProgram instanceof FlinkGroupProgram) {
        rewriteRules(programName, (FlinkOptimizeProgram<?>) internalProgram, rewrite);
        continue;
      }
      if (!(internalProgram instanceof FlinkRuleSetProgram)) {
        continue;
      }

      try {
        var f = internalProgram.getClass().getSuperclass().getDeclaredField("rules");
        f.setAccessible(true);

        var current = (List<RelOptRule>) f.get(internalProgram);
        var mutable = new ArrayList<>(current);

        rewrite.accept(mutable);
        if (!mutable.equals(current)) {
          f.set(internalProgram, List.copyOf(mutable));
        }
      } catch (ReflectiveOperationException e) {
        log.warn("Could not rewrite rules of program: " + programName, e);
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
