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
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.logical.FlinkProjectMergeRule;
import org.apache.flink.table.planner.plan.rules.logical.ProjectWindowTableFunctionTransposeRule;

@RequiredArgsConstructor
public class FlinkPlannerConfigBuilder {

  /** This is copied over from Flink, with the join transpose rules removed */
  private static final RuleSet PROJECT_RULES =
      RuleSets.ofList(
          // push a projection past a filter
          CoreRules.PROJECT_FILTER_TRANSPOSE,
          // merge projections
          FlinkProjectMergeRule.INSTANCE,
          // remove identity project
          CoreRules.PROJECT_REMOVE,
          // removes constant keys from an Agg
          CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
          // push project through a Union
          CoreRules.PROJECT_SET_OP_TRANSPOSE,
          // push a projection to the child of a WindowTableFunctionScan
          ProjectWindowTableFunctionTransposeRule.INSTANCE);

  private final CompilerConfig compilerConfig;
  private final SqrlFunctionCatalog sqrlFunctionCatalog;
  private final Configuration flinkConfig;

  public PlannerConfig build() {
    var calciteConfigBuilder = new CalciteConfigBuilder();
    calciteConfigBuilder.addSqlOperatorTable(sqrlFunctionCatalog.getOperatorTable());

    if (compilerConfig.disablePredicatePushdown()) {
      var sqrlStreamProgram = buildSqrlStreamProgram();
      calciteConfigBuilder.replaceStreamProgram(sqrlStreamProgram);
    }

    return calciteConfigBuilder.build();
  }

  private FlinkChainedProgram<StreamOptimizeContext> buildSqrlStreamProgram() {
    var origFlinkStreamProgram = FlinkStreamProgram.buildProgram(flinkConfig);
    var sqrlStreamProgram = new FlinkChainedProgram<StreamOptimizeContext>();

    for (var programName : origFlinkStreamProgram.getProgramNames()) {
      if (!programName.equals(FlinkStreamProgram.PREDICATE_PUSHDOWN())) {
        sqrlStreamProgram.addLast(programName, origFlinkStreamProgram.get(programName).get());
      }
    }

    return sqrlStreamProgram;
  }
}
