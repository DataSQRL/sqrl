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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.stream.flink.sql.rules.SqrlCalcMergeRule;
import com.datasqrl.engine.stream.flink.sql.rules.SqrlMiniBatchIntervalInferRule;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkRuleSetProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.rules.logical.FlinkCalcMergeRule;
import org.apache.flink.table.planner.plan.rules.physical.stream.MiniBatchIntervalInferRule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Tuple2;

class FlinkPlannerConfigBuilderTest {

  private static final List<String> DDL =
      List.of(
          """
          CREATE TABLE orders (
            id INT,
            currency STRING,
            ts TIMESTAMP_LTZ(3),
            WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
          ) WITH ('connector' = 'datagen')
          """,
          """
          CREATE TABLE rates (
            currency STRING,
            rate DECIMAL(10, 2),
            ts TIMESTAMP_LTZ(3),
            WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
          ) WITH ('connector' = 'datagen')
          """,
          """
          CREATE VIEW versioned_rates AS
          SELECT currency, rate, ts FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY ts DESC) AS rn FROM rates
          ) WHERE rn = 1
          """);

  private static final String QUERY =
      """
      SELECT o.currency, COUNT(*) AS cnt, SUM(r.rate) AS total
      FROM orders o
      LEFT JOIN versioned_rates FOR SYSTEM_TIME AS OF o.ts AS r ON o.currency = r.currency
      GROUP BY o.currency
      """;

  private static final String NESTED_CALCS_QUERY =
      """
      SELECT * FROM (
        SELECT id, currency FROM (
          SELECT id, currency, ts FROM orders WHERE id > 1
        ) WHERE id > 1
      )
      """;

  private static final String RENAMING_CALCS_QUERY =
      """
      SELECT id AS order_id, currency AS ccy FROM (
        SELECT id, currency, id + 1 AS next_id FROM orders WHERE currency <> 'EUR'
      ) WHERE next_id > 2
      """;

  @ParameterizedTest
  @EnumSource(PredicatePushdownRules.class)
  void givenAnyPushdownRules_whenBuild_thenPhysicalRewriteUsesSqrlMiniBatchRule(
      PredicatePushdownRules rules) {
    var flinkConf = new Configuration();
    var plannerConfig =
        (CalciteConfig) new FlinkPlannerConfigBuilder(config(rules), flinkConf).build();
    var physicalRewrite =
        plannerConfig.getStreamProgram().get().get(FlinkStreamProgram.PHYSICAL_REWRITE()).get();

    var ruleSetPrograms =
        subPrograms(physicalRewrite).stream()
            .map(Tuple2::_1)
            .filter(FlinkHepRuleSetProgram.class::isInstance)
            .map(FlinkHepRuleSetProgram.class::cast)
            .toList();

    assertThat(ruleSetPrograms).isNotEmpty();
    assertThat(ruleSetPrograms)
        .filteredOn(p -> p.contains(SqrlMiniBatchIntervalInferRule.INSTANCE))
        .hasSize(1);
    assertThat(ruleSetPrograms)
        .filteredOn(p -> p.contains(MiniBatchIntervalInferRule.INSTANCE))
        .isEmpty();
  }

  @ParameterizedTest
  @EnumSource(PredicatePushdownRules.class)
  void givenAnyPushdownRules_whenBuild_thenCalcMergeRulesAreReplaced(PredicatePushdownRules rules) {
    var flinkConf = new Configuration();
    var plannerConfig =
        (CalciteConfig) new FlinkPlannerConfigBuilder(config(rules), flinkConf).build();
    var streamProgram = plannerConfig.getStreamProgram().get();

    var logical = (FlinkRuleSetProgram<?>) streamProgram.get(FlinkStreamProgram.LOGICAL()).get();
    var physical = (FlinkRuleSetProgram<?>) streamProgram.get(FlinkStreamProgram.PHYSICAL()).get();
    var logicalRewrite =
        subPrograms(streamProgram.get(FlinkStreamProgram.LOGICAL_REWRITE()).get()).stream()
            .map(Tuple2::_1)
            .map(FlinkRuleSetProgram.class::cast)
            .toList();

    assertThat(logical.contains(SqrlCalcMergeRule.INSTANCE)).isTrue();
    assertThat(logical.contains(FlinkCalcMergeRule.INSTANCE)).isFalse();
    assertThat(physical.contains(SqrlCalcMergeRule.STREAM_PHYSICAL_INSTANCE)).isTrue();
    assertThat(physical.contains(FlinkCalcMergeRule.STREAM_PHYSICAL_INSTANCE)).isFalse();
    assertThat(logicalRewrite).filteredOn(p -> p.contains(SqrlCalcMergeRule.INSTANCE)).hasSize(1);
    assertThat(logicalRewrite).filteredOn(p -> p.contains(FlinkCalcMergeRule.INSTANCE)).isEmpty();
  }

  @Test
  void givenMiniBatchTemporalJoin_whenExplain_thenPlanEqualsFlinkDefaultProgram() {
    var sqrlPlan = explain(true, QUERY);
    var flinkPlan = explain(false, QUERY);

    assertThat(sqrlPlan).contains("MiniBatchAssigner").contains("TemporalJoin").contains("Rank");
    assertThat(sqrlPlan).isEqualTo(flinkPlan);
  }

  @ParameterizedTest
  @ValueSource(strings = {NESTED_CALCS_QUERY, RENAMING_CALCS_QUERY})
  void givenNestedCalcs_whenExplain_thenPlanEqualsFlinkDefaultProgram(String query) {
    var sqrlPlan = explain(true, query);
    var flinkPlan = explain(false, query);

    assertThat(sqrlPlan).contains("Calc(");
    assertThat(sqrlPlan).isEqualTo(flinkPlan);
  }

  private String explain(boolean sqrlPlannerConfig, String query) {
    var flinkConf = new Configuration();
    flinkConf.setString("table.exec.mini-batch.enabled", "true");
    flinkConf.setString("table.exec.mini-batch.allow-latency", "5 s");
    flinkConf.setString("table.exec.mini-batch.size", "1000");

    var tEnv = TableEnvironment.create(flinkConf);
    if (sqrlPlannerConfig) {
      var plannerConfig =
          new FlinkPlannerConfigBuilder(config(PredicatePushdownRules.DEFAULT), flinkConf).build();
      tEnv.getConfig().setPlannerConfig(plannerConfig);
    }
    DDL.forEach(tEnv::executeSql);

    return tEnv.explainSql(query, ExplainFormat.TEXT);
  }

  private CompilerConfig config(PredicatePushdownRules rules) {
    var compilerConf = mock(CompilerConfig.class);
    when(compilerConf.predicatePushdownRules()).thenReturn(rules);
    return compilerConf;
  }

  @SuppressWarnings("unchecked")
  private List<Tuple2<FlinkOptimizeProgram<?>, String>> subPrograms(
      FlinkOptimizeProgram<?> groupProgram) {
    try {
      var f = groupProgram.getClass().getDeclaredField("programs");
      f.setAccessible(true);
      return (List<Tuple2<FlinkOptimizeProgram<?>, String>>) f.get(groupProgram);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
