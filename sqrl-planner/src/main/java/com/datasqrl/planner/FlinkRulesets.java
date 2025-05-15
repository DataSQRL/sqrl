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

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.logical.FlinkProjectMergeRule;
import org.apache.flink.table.planner.plan.rules.logical.ProjectWindowTableFunctionTransposeRule;

public class FlinkRulesets {

  /** This is copied over from Flink. We removed the join transpose rules */
  public static final RuleSet PROJECT_RULES =
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

  private static void setOptimizerRules(
      CalciteConfigBuilder calciteConfigBuilder, TableConfig tableConfig) {
    var originalProgram = FlinkStreamProgram.buildProgram(tableConfig);
    var modifiedProgram = new FlinkChainedProgram<StreamOptimizeContext>();

    // Iterate over program names and replace PROJECT_REWRITE
    //    for (String programName : originalProgram.getProgramNames()) {
    //      if (programName.equals(FlinkStreamProgram.PROJECT_REWRITE())) {
    //        modifiedProgram.addLast(
    //            FlinkStreamProgram.PROJECT_REWRITE(),
    //            FlinkHepRuleSetProgramBuilder.newBuilder()
    //                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
    //                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
    //                .add(FlinkRulesets.PROJECT_RULES)
    //                .build()
    //        );
    //      } else {
    //        modifiedProgram.addLast(programName, originalProgram.get(programName).get());
    //      }
    //    }
    //    calciteConfigBuilder.replaceStreamProgram(modifiedProgram);
  }
}
