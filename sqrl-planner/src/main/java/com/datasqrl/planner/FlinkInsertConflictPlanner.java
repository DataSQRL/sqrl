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
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.types.RowKind;

/**
 * Resolves generated Flink {@code ON CONFLICT} clauses from the optimized sink plans: insert-only
 * sinks get no clause, and sinks whose upsert key is not contained in the primary key get the
 * clause the forked Flink planner requires.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class FlinkInsertConflictPlanner {

  private final List<PendingInsert> pendingInserts = new ArrayList<>();

  private final RuntimeExecutionMode executionMode;
  private final StreamTableEnvironmentImpl tEnv;
  private final Builder planBuilder;

  /** Adds a generated insert and registers upsert-capable sinks for post-planning resolution. */
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
   * Replaces generated inserts with conflict behavior derived from each Flink-optimized sink plan.
   */
  void resolve() {
    if (pendingInserts.isEmpty()) {
      return;
    }

    // Sink planning inspects StreamPhysicalSink, which is not used for batch plans.
    var sinkPlans =
        executionMode == RuntimeExecutionMode.STREAMING
            ? planSinks()
            : pendingInserts.stream().map(insert -> Optional.<StreamPhysicalSink>empty()).toList();

    for (var i = 0; i < pendingInserts.size(); i++) {
      var pendingInsert = pendingInserts.get(i);
      var conflictBehavior = resolveConflictBehavior(pendingInsert.table(), sinkPlans.get(i));

      planBuilder.replaceInsert(
          pendingInsert.batchIdx(),
          pendingInsert.insertIdx(),
          FlinkSqlNodes.createInsert(
              pendingInsert.selectQuery(), pendingInsert.targetTableId(), conflictBehavior));
    }
  }

  boolean hasPendingInserts() {
    return !pendingInserts.isEmpty();
  }

  /** Compiles the generated statement set. */
  CompiledPlan compilePlan() {
    var execute = planBuilder.getExecuteStatements();
    var statements = RelToFlinkSql.convertToSqlString(execute);
    var statementSet =
        (StatementSetOperation) tEnv.getParser().parse(statements.get(0) + ";").get(0);

    try {
      return tEnv.compilePlan(statementSet.getOperations());
    } catch (Exception e) {
      throw new FlinkCompileException(withStatements(statements), e);
    }
  }

  /**
   * Preserves explicit behavior and otherwise derives the clause from the optimized sink plan,
   * mirroring the forked Flink planner's validation: append and retract sinks reject the clause,
   * upsert sinks whose primary key does not contain the upsert key require one (falling back to
   * {@code DO NOTHING}), and insert-only inputs need none.
   */
  private Optional<SqlInsertConflictBehavior> resolveConflictBehavior(
      TableAnalysis table, Optional<StreamPhysicalSink> sinkPlan) {

    if (table.getInsertConflictBehavior().isPresent()) {
      return table
          .getInsertConflictBehavior()
          .map(FlinkInsertConflictPlanner::toSqlConflictBehavior);
    }

    if (sinkPlan.isEmpty()) {
      // Batch plans have no stream-physical sink and no conflict validation in the fork.
      return automaticConflictBehavior(table);
    }

    var sink = sinkPlan.get();
    var inputMode = inputChangelogMode(sink).orElse(ChangelogMode.all());
    var sinkMode = sink.tableSink().getChangelogMode(inputMode);
    if (sinkMode.containsOnly(RowKind.INSERT) || sinkMode.contains(RowKind.UPDATE_BEFORE)) {
      return Optional.empty();
    }

    if (!sink.primaryKeysContainsUpsertKey()) {
      return automaticConflictBehavior(table)
          .or(() -> Optional.of(SqlInsertConflictBehavior.NOTHING));
    }

    return inputMode.containsOnly(RowKind.INSERT)
        ? Optional.empty()
        : automaticConflictBehavior(table);
  }

  /**
   * Plans the pending inserts without conflict clauses and returns their optimized sink nodes. The
   * fork's upsert-key validation is disabled during planning since this pass determines the very
   * clauses that validation requires.
   */
  private List<Optional<StreamPhysicalSink>> planSinks() {
    var config = tEnv.getConfig();
    var requireOnConflict = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT);
    config.set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, false);

    try {
      var insertOperations =
          pendingInserts.stream()
              .map(
                  insert ->
                      FlinkSqlNodes.createInsert(insert.selectQuery(), insert.targetTableId()))
              .map(RelToFlinkSql::convertToString)
              .map(sql -> tEnv.getParser().parse(sql).get(0))
              .toList();

      // getExplainGraphs translates and optimizes the inserts; _2() contains optimized RelNodes.
      var optimizedPlans =
          ((PlannerBase) tEnv.getPlanner()).getExplainGraphs(insertOperations)._2();

      var sinkPlans = new ArrayList<Optional<StreamPhysicalSink>>();
      var iterator = optimizedPlans.iterator();
      for (var pendingInsert : pendingInserts) {
        if (!iterator.hasNext()) {
          throw new IllegalStateException(
              "Missing optimized plan for INSERT INTO " + pendingInsert.targetTableId());
        }

        var plan = iterator.next();
        sinkPlans.add(
            plan instanceof StreamPhysicalSink sinkPlan ? Optional.of(sinkPlan) : Optional.empty());
      }

      return sinkPlans;

    } catch (Exception e) {
      throw new FlinkCompileException(planBuilder.getFlinkSql(), e);
    } finally {
      config.set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, requireOnConflict);
    }
  }

  private static Optional<ChangelogMode> inputChangelogMode(StreamPhysicalSink sinkPlan) {
    if (sinkPlan.getInput() instanceof StreamPhysicalRel input) {
      var changelogMode = ChangelogPlanUtils.getChangelogMode(input);
      if (changelogMode.isDefined()) {
        return Optional.of(changelogMode.get());
      }
    }

    return Optional.empty();
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
      TableAnalysis table,
      int batchIdx,
      int insertIdx) {}
}
