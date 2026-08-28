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

import com.datasqrl.engine.stream.flink.FlinkCalciteParser;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Optional;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises conflict-clause resolution against the real Flink planner: an updating query whose
 * upsert key is not contained in the sink primary key must receive an {@code ON CONFLICT} clause,
 * insert-only queries must not, and the resulting statement set must compile without retries.
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
    conflictPlanner =
        new FlinkInsertConflictPlanner(RuntimeExecutionMode.STREAMING, tEnv, planBuilder);

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
  void givenUpsertKeyNotInSinkPrimaryKey_whenResolve_thenAddsRequiredConflictClause() {
    // Upsert key [name] is not contained in the sink primary key [cnt]; the RELATION type has no
    // automatic behavior, so the required clause falls back to DO NOTHING.
    addInsert(aggregateQuery(), MISMATCH_SINK, table(TableType.RELATION));

    conflictPlanner.resolve();

    assertThat(insertSql(0)).contains("ON CONFLICT DO NOTHING");
    assertThat(conflictPlanner.compilePlan()).isNotNull();
  }

  @Test
  void givenInsertOnlyQueryWithUpsertKeyInPrimaryKey_whenResolve_thenOmitsConflictClause() {
    // Keep-first rowtime deduplication stays insert-only and derives upsert key [name].
    var dedupQuery =
        """
        SELECT name, CAST(1 AS BIGINT) AS cnt FROM (
          SELECT name, ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts ASC) AS rn FROM src
        ) WHERE rn = 1""";
    addInsert(dedupQuery, MATCHING_SINK, table(TableType.STREAM));

    conflictPlanner.resolve();

    assertThat(insertSql(0)).doesNotContain("ON CONFLICT");
    assertThat(conflictPlanner.compilePlan()).isNotNull();
  }

  @Test
  void givenUpsertKeyContainedInSinkPrimaryKey_whenResolve_thenOmitsClauseForRelationTable() {
    addInsert(aggregateQuery(), MATCHING_SINK, table(TableType.RELATION));

    conflictPlanner.resolve();

    assertThat(insertSql(0)).doesNotContain("ON CONFLICT");
    assertThat(conflictPlanner.compilePlan()).isNotNull();
  }

  @Test
  void givenExplicitConflictBehavior_whenResolve_thenPreservesBehavior() {
    addInsert(
        aggregateQuery(),
        MISMATCH_SINK,
        table(TableType.RELATION, Optional.of(ConflictBehavior.DEDUPLICATE)));

    conflictPlanner.resolve();

    assertThat(insertSql(0)).contains("ON CONFLICT DO DEDUPLICATE");
    assertThat(conflictPlanner.compilePlan()).isNotNull();
  }

  @Test
  void givenStateTableWithUpdatingQuery_whenResolve_thenDeduplicates() {
    addInsert(aggregateQuery(), MATCHING_SINK, table(TableType.STATE));

    conflictPlanner.resolve();

    assertThat(insertSql(0)).contains("ON CONFLICT DO DEDUPLICATE");
    assertThat(conflictPlanner.compilePlan()).isNotNull();
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
