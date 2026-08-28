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

import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.planner.FlinkPhysicalPlan.Builder;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.tables.SourceSinkTableAnalysis;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.FlinkCompileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.Strings;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.types.RowKind;

/**
 * Resolves generated Flink {@code ON CONFLICT} clauses from optimized query changelog modes and
 * final sink validation.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class FlinkInsertConflictPlanner {

  private final List<PendingInsert> pendingInserts = new ArrayList<>();
  private final Set<ObjectIdentifier> upsertKeyFallbackTargets = new HashSet<>();

  private final RuntimeExecutionMode executionMode;
  private final StreamTableEnvironmentImpl tEnv;
  private final Builder planBuilder;

  /** Adds a generated insert and registers upsert-capable sinks for post-inference resolution. */
  void addInsert(
      SqlNode selectQuery,
      ObjectIdentifier sinkTableId,
      TableAnalysis table,
      boolean isUpsertSink) {

    var insert = FlinkSqlNodes.createInsert(selectQuery, sinkTableId);
    if (!isUpsertSink) {
      planBuilder.addInsert(insert);
      return;
    }

    var batchIdx = planBuilder.currentBatch();
    var insertIdx = planBuilder.addInsert(insert);
    pendingInserts.add(new PendingInsert(selectQuery, sinkTableId, table, batchIdx, insertIdx));
  }

  /**
   * Replaces generated inserts with conflict behavior derived from each Flink-optimized sink
   * changelog.
   */
  void resolve() {
    if (pendingInserts.isEmpty()) {
      return;
    }

    // Changelog inference inspects StreamPhysicalSink, which is not used for batch plans.
    var changelogModes =
        executionMode == RuntimeExecutionMode.STREAMING
            ? inferSinkChangelogModes()
            : Map.<ObjectIdentifier, ChangelogMode>of();

    for (var pendingInsert : pendingInserts) {
      var changelogMode =
          changelogModes.getOrDefault(pendingInsert.targetTableId(), ChangelogMode.all());
      var conflictBehavior = resolveConflictBehavior(pendingInsert.table(), changelogMode);

      planBuilder.replaceInsert(
          pendingInsert.batchIdx(),
          pendingInsert.insertIdx(),
          FlinkSqlNodes.createInsert(
              pendingInsert.selectQuery(), pendingInsert.targetTableId(), conflictBehavior));
    }
  }

  /**
   * Compiles the generated statement set, applying automatic conflict behavior when Flink reports
   * an upsert key that does not match a sink primary key.
   */
  CompiledPlan compilePlan() {
    while (true) {
      var execute = planBuilder.getExecuteStatements();
      var statements = RelToFlinkSql.convertToSqlString(execute);

      try {
        var statementSet =
            (StatementSetOperation) tEnv.getParser().parse(statements.get(0) + ";").get(0);

        return tEnv.compilePlan(statementSet.getOperations());

      } catch (Exception e) {
        if (!resolveUpsertKeyConflict(e)) {
          var flinkSql = new ArrayList<>(planBuilder.getFlinkSql());
          flinkSql.addAll(statements);
          throw new FlinkCompileException(flinkSql, e);
        }
      }
    }
  }

  boolean hasPendingInserts() {
    return !pendingInserts.isEmpty();
  }

  /**
   * Adds conflict behavior when final Flink compilation rejects an insert because its upsert key
   * differs from the sink primary key.
   */
  boolean resolveUpsertKeyConflict(Throwable error) {
    var pendingInsert =
        pendingInserts.stream()
            .filter(insert -> !upsertKeyFallbackTargets.contains(insert.targetTableId()))
            .filter(insert -> hasUpsertKeyConflict(error, insert.targetTableId()))
            .findFirst();

    if (pendingInsert.isEmpty()) {
      return false;
    }

    var insert = pendingInsert.get();
    var conflictBehavior =
        insert
            .table()
            .getInsertConflictBehavior()
            .map(FlinkInsertConflictPlanner::toSqlConflictBehavior)
            .or(() -> automaticConflictBehavior(insert.table()))
            .or(() -> Optional.of(SqlInsertConflictBehavior.NOTHING));

    planBuilder.replaceInsert(
        insert.batchIdx(),
        insert.insertIdx(),
        FlinkSqlNodes.createInsert(insert.selectQuery(), insert.targetTableId(), conflictBehavior));

    upsertKeyFallbackTargets.add(insert.targetTableId());

    return true;
  }

  /**
   * Preserves explicit behavior or selects automatic behavior from the sink changelog, omitting it
   * for insert-only sinks.
   */
  private Optional<SqlInsertConflictBehavior> resolveConflictBehavior(
      TableAnalysis table, ChangelogMode changelogMode) {

    if (table.getInsertConflictBehavior().isPresent()) {
      return table
          .getInsertConflictBehavior()
          .map(FlinkInsertConflictPlanner::toSqlConflictBehavior);
    }

    if (changelogMode.containsOnly(RowKind.INSERT)) {
      return Optional.empty();
    }

    return automaticConflictBehavior(table);
  }

  /**
   * Plans generated queries without sink-specific conflict validation and extracts their changelog
   * modes.
   */
  private Map<ObjectIdentifier, ChangelogMode> inferSinkChangelogModes() {
    var selectSql =
        pendingInserts.stream()
            .map(PendingInsert::selectQuery)
            .map(RelToFlinkSql::convertToString)
            .toList();
    try {
      var queryOperations =
          selectSql.stream().map(sql -> tEnv.getParser().parse(sql).get(0)).toList();

      // getExplainGraphs translates and optimizes the queries; _2() contains optimized RelNodes.
      var optimizedPlans = ((PlannerBase) tEnv.getPlanner()).getExplainGraphs(queryOperations)._2();

      var changelogModes = new HashMap<ObjectIdentifier, ChangelogMode>();
      var iterator = optimizedPlans.iterator();
      for (var pendingInsert : pendingInserts) {
        if (!iterator.hasNext()) {
          throw new IllegalStateException(
              "Missing optimized plan for INSERT INTO " + pendingInsert.targetTableId());
        }

        var plan = iterator.next();
        if (plan instanceof StreamPhysicalRel streamPlan) {
          var changelogMode = ChangelogPlanUtils.getChangelogMode(streamPlan);
          if (changelogMode.isDefined()) {
            changelogModes.put(pendingInsert.targetTableId(), changelogMode.get());
          }
        }
      }

      return changelogModes;

    } catch (Exception e) {
      var flinkSql = new ArrayList<>(planBuilder.getFlinkSql());
      flinkSql.addAll(selectSql);
      throw new FlinkCompileException(flinkSql, e);
    }
  }

  private static Optional<SqlInsertConflictBehavior> automaticConflictBehavior(
      TableAnalysis table) {

    return switch (table.getType()) {
      case VERSIONED_STATE, STATE, STATIC -> Optional.of(SqlInsertConflictBehavior.DEDUPLICATE);
      case STREAM ->
          Optional.of(
              hasWatermarkOnSource(table)
                  ? SqlInsertConflictBehavior.NOTHING
                  : SqlInsertConflictBehavior.DEDUPLICATE);
      default -> Optional.empty();
    };
  }

  /**
   * Returns whether the exception chain contains Flink's upsert-key validation failure for the
   * given sink.
   */
  private static boolean hasUpsertKeyConflict(Throwable error, ObjectIdentifier targetTableId) {
    var target = "'" + targetTableId.asSummaryString() + "'";
    var cause = error;
    while (cause != null) {
      var msg = cause.getMessage();

      if (Strings.CS.contains(msg, "The query has an upsert key that differs from the primary key")
          && Strings.CS.contains(msg, target)) {
        return true;
      }
      cause = cause.getCause();
    }

    return false;
  }

  private static SqlInsertConflictBehavior toSqlConflictBehavior(
      InsertConflictStrategy.ConflictBehavior behavior) {

    return switch (behavior) {
      case ERROR -> SqlInsertConflictBehavior.ERROR;
      case NOTHING -> SqlInsertConflictBehavior.NOTHING;
      case DEDUPLICATE -> SqlInsertConflictBehavior.DEDUPLICATE;
    };
  }

  private static boolean hasWatermarkOnSource(TableOrFunctionAnalysis table) {
    if (table instanceof TableAnalysis tableAnalysis
        && !tableAnalysis.isSourceOrSink()
        && !tableAnalysis.getFromTables().isEmpty()) {
      return tableAnalysis.getFromTables().stream()
          .allMatch(FlinkInsertConflictPlanner::hasWatermarkOnSource);
    }

    if (table instanceof SqrlTableFunction tableFunction) {
      return hasWatermarkOnSource(tableFunction.getFunctionAnalysis());
    }

    if (table instanceof TableAnalysis sourceTable) {
      return sourceTable
          .getSourceSinkTable()
          .map(SourceSinkTableAnalysis::schema)
          .map(ResolvedSchema::getWatermarkSpecs)
          .map(watermarkSpecs -> !watermarkSpecs.isEmpty())
          .orElse(false);
    }

    return false;
  }

  private record PendingInsert(
      SqlNode selectQuery,
      ObjectIdentifier targetTableId,
      TableAnalysis table,
      int batchIdx,
      int insertIdx) {}
}
