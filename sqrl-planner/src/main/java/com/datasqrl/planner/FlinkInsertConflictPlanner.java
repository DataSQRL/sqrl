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
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.StatementSetOperation;

/**
 * Resolves generated Flink {@code ON CONFLICT} clauses while the statement set is compiled: the
 * {@link FlinkInsertConflictProgram} installed in the stream optimizer derives each clause from the
 * optimized sink plan, and the generated inserts are rewritten with the resolved clauses
 * afterwards.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class FlinkInsertConflictPlanner {

  private final List<PendingInsert> pendingInserts = new ArrayList<>();

  private final StreamTableEnvironmentImpl tEnv;
  private final Builder planBuilder;

  @Getter
  private final FlinkInsertConflictProgram conflictProgram = new FlinkInsertConflictProgram();

  /**
   * Adds a generated insert. Explicit conflict behavior is applied immediately; other
   * upsert-capable sinks are registered for resolution from the optimized plan.
   */
  void addInsert(
      SqlNode selectQuery,
      ObjectIdentifier sinkTableId,
      TableAnalysis table,
      boolean isUpsertSink) {

    var explicitBehavior =
        isUpsertSink
            ? table
                .getInsertConflictBehavior()
                .map(FlinkInsertConflictPlanner::toSqlConflictBehavior)
            : Optional.<SqlInsertConflictBehavior>empty();
    if (!isUpsertSink || explicitBehavior.isPresent()) {
      planBuilder.addInsert(FlinkSqlNodes.createInsert(selectQuery, sinkTableId, explicitBehavior));
      return;
    }

    var batchIdx = planBuilder.currentBatch();
    var insertIdx = planBuilder.addInsert(FlinkSqlNodes.createInsert(selectQuery, sinkTableId));
    pendingInserts.add(
        new PendingInsert(
            selectQuery, sinkTableId, automaticConflictBehavior(table), batchIdx, insertIdx));
  }

  /** Applies the automatic conflict behavior to plans that are not compiled by Flink. */
  void resolve() {
    pendingInserts.forEach(pendingInsert -> replaceInsert(pendingInsert, pendingInsert.fallback()));
  }

  boolean hasPendingInserts() {
    return !pendingInserts.isEmpty();
  }

  /**
   * Compiles the generated statement set once and rewrites the pending inserts with the conflict
   * clauses resolved during that optimization. The fork's upsert-key validation is disabled for the
   * pass since the pending inserts carry no clause yet; the program re-validates the other sinks.
   */
  CompiledPlan compilePlan() {
    var execute = planBuilder.getExecuteStatements();
    var statements = RelToFlinkSql.convertToSqlString(execute);
    var statementSet =
        (StatementSetOperation) tEnv.getParser().parse(statements.get(0) + ";").get(0);

    var config = tEnv.getConfig();
    var requireOnConflict = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT);
    config.set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, false);
    conflictProgram.setRequireOnConflict(requireOnConflict);
    pendingInserts.forEach(
        pendingInsert ->
            conflictProgram.registerSink(pendingInsert.targetTableId(), pendingInsert.fallback()));

    try {
      var compiledPlan = tEnv.compilePlan(statementSet.getOperations());
      pendingInserts.forEach(
          pendingInsert ->
              replaceInsert(
                  pendingInsert, conflictProgram.resolvedBehavior(pendingInsert.targetTableId())));
      return compiledPlan;

    } catch (Exception e) {
      throw new FlinkCompileException(withStatements(statements), e);

    } finally {
      config.set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, requireOnConflict);
    }
  }

  private void replaceInsert(
      PendingInsert pendingInsert, Optional<SqlInsertConflictBehavior> conflictBehavior) {

    planBuilder.replaceInsert(
        pendingInsert.batchIdx(),
        pendingInsert.insertIdx(),
        FlinkSqlNodes.createInsert(
            pendingInsert.selectQuery(), pendingInsert.targetTableId(), conflictBehavior));
  }

  private List<String> withStatements(List<String> statements) {
    var flinkSql = new ArrayList<>(planBuilder.getFlinkSql());
    flinkSql.addAll(statements);
    return flinkSql;
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
      Optional<SqlInsertConflictBehavior> fallback,
      int batchIdx,
      int insertIdx) {}
}
