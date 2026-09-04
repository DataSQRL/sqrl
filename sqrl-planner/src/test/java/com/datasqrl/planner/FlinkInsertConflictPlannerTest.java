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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.stream.flink.FlinkCalciteParser;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises conflict-clause resolution against the real Flink planner: an updating query whose
 * upsert key is not contained in the sink primary key must receive an {@code ON CONFLICT} clause,
 * insert-only queries must not, and the clauses must be resolved by the single compilation pass.
 */
class FlinkInsertConflictPlannerTest {

  private static final ObjectIdentifier MISMATCH_SINK =
      ObjectIdentifier.of("default_catalog", "default_database", "snk_cnt_pk");
  private static final ObjectIdentifier MATCHING_SINK =
      ObjectIdentifier.of("default_catalog", "default_database", "snk_name_pk");

  private StreamTableEnvironmentImpl tEnv;
  private FlinkPhysicalPlan.Builder planBuilder;
  private FlinkInsertConflictPlanner conflictPlanner;

  @BeforeEach
  void setUp() {
    var sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    tEnv =
        (StreamTableEnvironmentImpl)
            StreamTableEnvironment.create(
                sEnv, EnvironmentSettings.newInstance().inStreamingMode().build());
    planBuilder = new FlinkPhysicalPlan.Builder(new Configuration());
    conflictPlanner = new FlinkInsertConflictPlanner(tEnv, planBuilder);
    var compilerConfig = mock(CompilerConfig.class);
    when(compilerConfig.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);
    tEnv.getConfig()
        .setPlannerConfig(
            new FlinkPlannerConfigBuilder(
                    compilerConfig, null, new Configuration(), conflictPlanner.getConflictProgram())
                .build());

    tEnv.executeSql(
        """
        CREATE TABLE src (
          name STRING NOT NULL,
          ts TIMESTAMP(3),
          WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        ) WITH ('connector' = 'datagen')""");
    createJdbcSink("snk_cnt_pk", "PRIMARY KEY (cnt) NOT ENFORCED");
    createJdbcSink("snk_name_pk", "PRIMARY KEY (name) NOT ENFORCED");
  }

  @Test
  void givenUpsertKeyNotInSinkPrimaryKey_whenCompile_thenAddsRequiredConflictClause() {
    // Upsert key [name] is not contained in the sink primary key [cnt]; the RELATION type has no
    // automatic behavior, so the required clause falls back to DO NOTHING.
    addInsert(aggregateQuery(), MISMATCH_SINK, table(TableType.RELATION));

    var compiledPlan = conflictPlanner.compilePlan();

    assertThat(insertSql(0)).contains("ON CONFLICT DO NOTHING");
    assertThat(compiledPlan.asJsonString()).contains("\"conflictStrategy\"");
  }

  @Test
  void givenUpsertKeyMismatchWithRetracts_whenResolve_thenAddsDeduplicateClause() {
    var sink = mock(StreamPhysicalSink.class);
    var tableSink = mock(DynamicTableSink.class);
    when(sink.tableSink()).thenReturn(tableSink);
    when(sink.primaryKeysContainsUpsertKey()).thenReturn(false);
    when(tableSink.getChangelogMode(any(ChangelogMode.class)))
        .thenReturn(ChangelogMode.newBuilder().addContainedKind(RowKind.UPDATE_BEFORE).build());

    assertThat(
            FlinkInsertConflictProgram.resolveConflictBehavior(
                sink, Optional.of(SqlInsertConflictBehavior.DEDUPLICATE)))
        .contains(SqlInsertConflictBehavior.DEDUPLICATE);
  }

  @Test
  void givenUpsertKeyMismatchWithAppendOnlySink_whenResolve_thenOmitsConflictClause() {
    var sink = mock(StreamPhysicalSink.class);
    var tableSink = mock(DynamicTableSink.class);
    when(sink.tableSink()).thenReturn(tableSink);
    when(sink.primaryKeysContainsUpsertKey()).thenReturn(false);
    when(tableSink.getChangelogMode(any(ChangelogMode.class)))
        .thenReturn(ChangelogMode.insertOnly());

    assertThat(
            FlinkInsertConflictProgram.resolveConflictBehavior(
                sink, Optional.of(SqlInsertConflictBehavior.DEDUPLICATE)))
        .isEmpty();
  }

  @Test
  void givenInsertOnlyQueryWithUpsertKeyInPrimaryKey_whenCompile_thenOmitsConflictClause() {
    // Keep-first rowtime deduplication stays insert-only and derives upsert key [name].
    var dedupQuery =
        """
        SELECT name, CAST(1 AS BIGINT) AS cnt FROM (
          SELECT name, ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts ASC) AS rn FROM src
        ) WHERE rn = 1""";
    addInsert(dedupQuery, MATCHING_SINK, table(TableType.STREAM));

    assertThat(conflictPlanner.compilePlan()).isNotNull();
    assertThat(insertSql(0)).doesNotContain("ON CONFLICT");
  }

  @Test
  void givenUpsertKeyContainedInSinkPrimaryKey_whenCompile_thenOmitsClauseForRelationTable() {
    addInsert(aggregateQuery(), MATCHING_SINK, table(TableType.RELATION));

    assertThat(conflictPlanner.compilePlan()).isNotNull();
    assertThat(insertSql(0)).doesNotContain("ON CONFLICT");
  }

  @Test
  void givenExplicitConflictBehavior_whenCompile_thenPreservesBehavior() {
    addInsert(
        aggregateQuery(),
        MISMATCH_SINK,
        table(TableType.RELATION, Optional.of(ConflictBehavior.DEDUPLICATE)));

    assertThat(insertSql(0)).contains("ON CONFLICT DO DEDUPLICATE");
    assertThat(conflictPlanner.compilePlan()).isNotNull();
    assertThat(insertSql(0)).contains("ON CONFLICT DO DEDUPLICATE");
  }

  @Test
  void givenStateTableWithUpdatingQuery_whenCompile_thenDeduplicates() {
    addInsert(aggregateQuery(), MATCHING_SINK, table(TableType.STATE));

    assertThat(conflictPlanner.compilePlan()).isNotNull();
    assertThat(insertSql(0)).contains("ON CONFLICT DO DEDUPLICATE");
  }

  private void createJdbcSink(String name, String primaryKey) {
    tEnv.executeSql(
        """
        CREATE TABLE %s (
          name STRING NOT NULL,
          cnt BIGINT NOT NULL,
          %s
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:postgresql://localhost:5432/unused',
          'table-name' = '%s'
        )"""
            .formatted(name, primaryKey, name));
  }

  private static String aggregateQuery() {
    return "SELECT name, COUNT(*) AS cnt FROM src GROUP BY name";
  }

  private void addInsert(String selectSql, ObjectIdentifier sinkTableId, TableAnalysis table) {
    conflictPlanner.addInsert(
        FlinkCalciteParser.parseSql(selectSql, tEnv), sinkTableId, table, true);
  }

  private String insertSql(int insertIdx) {
    return RelToFlinkSql.convertToString(planBuilder.getStatementSets().get(0).get(insertIdx));
  }

  private static TableAnalysis table(TableType type) {
    return table(type, Optional.empty());
  }

  private static TableAnalysis table(
      TableType type, Optional<ConflictBehavior> insertConflictBehavior) {
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("catalog", "database", "table"))
        .type(type)
        .insertConflictBehavior(insertConflictBehavior)
        .build();
  }
}
