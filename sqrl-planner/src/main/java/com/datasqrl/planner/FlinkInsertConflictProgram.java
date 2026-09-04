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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTraitDef;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.types.RowKind;

/**
 * Runs right after the forked Flink planner's changelog mode inference and resolves the {@code ON
 * CONFLICT} clause of each registered sink from the inferred sink plan, so that the statement set
 * is optimized only once. It applies the fork's clause validation and upsert-materialize rule to
 * the resolved clause and re-validates the sinks the fork could not check while the pending sinks
 * were planned without a clause.
 */
final class FlinkInsertConflictProgram implements FlinkOptimizeProgram<StreamOptimizeContext> {

  private final Map<ObjectIdentifier, Optional<SqlInsertConflictBehavior>> fallbacks =
      new HashMap<>();
  private final Map<ObjectIdentifier, Optional<SqlInsertConflictBehavior>> resolved =
      new HashMap<>();
  private boolean requireOnConflict =
      ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT.defaultValue();

  void registerSink(ObjectIdentifier sinkTableId, Optional<SqlInsertConflictBehavior> fallback) {
    fallbacks.put(sinkTableId, fallback);
  }

  void setRequireOnConflict(boolean requireOnConflict) {
    this.requireOnConflict = requireOnConflict;
  }

  Optional<SqlInsertConflictBehavior> resolvedBehavior(ObjectIdentifier sinkTableId) {
    var behavior = resolved.get(sinkTableId);
    if (behavior == null) {
      throw new IllegalStateException("Missing optimized plan for INSERT INTO " + sinkTableId);
    }

    return behavior;
  }

  @Override
  public RelNode optimize(RelNode root, StreamOptimizeContext context) {
    if (!(root instanceof StreamPhysicalSink sink)) {
      return root;
    }

    var sinkTableId = sink.contextResolvedTable().getIdentifier();
    var fallback = fallbacks.get(sinkTableId);
    if (fallback == null) {
      requireConflictClause(sink, context);
      return root;
    }

    var behavior = resolveConflictBehavior(sink, fallback);
    resolved.put(sinkTableId, behavior);
    if (behavior.isEmpty()) {
      return root;
    }

    var strategy = toConflictStrategy(behavior.get());
    validateUpsertSink(sink);
    if (strategy.getBehavior() != InsertConflictStrategy.ConflictBehavior.DEDUPLICATE) {
      validateSourcesHaveWatermarks(sink, strategy);
    }

    var upsertMaterialize =
        sink.upsertMaterialize()
            && !(materializeMode(context) == UpsertMaterialize.AUTO
                && inferredInputMode(sink).containsOnly(RowKind.INSERT)
                && strategy.getBehavior() == InsertConflictStrategy.ConflictBehavior.DEDUPLICATE);

    return new StreamPhysicalSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInput(),
        sink.hints(),
        sink.contextResolvedTable(),
        sink.tableSink(),
        sink.targetColumns(),
        sink.abilitySpecs(),
        upsertMaterialize,
        strategy);
  }

  /**
   * Derives the clause from the optimized sink plan: append and retract sinks reject the clause,
   * upsert sinks whose primary key does not contain the upsert key require one (falling back to
   * {@code DO NOTHING}), and insert-only inputs need none.
   */
  static Optional<SqlInsertConflictBehavior> resolveConflictBehavior(
      StreamPhysicalSink sink, Optional<SqlInsertConflictBehavior> fallback) {

    var inputMode = inputChangelogMode(sink).orElse(ChangelogMode.all());
    var sinkMode = sink.tableSink().getChangelogMode(inputMode);
    if (sinkMode.containsOnly(RowKind.INSERT)) {
      return Optional.empty();
    }

    if (!sink.primaryKeysContainsUpsertKey()) {
      return fallback.or(() -> Optional.of(SqlInsertConflictBehavior.NOTHING));
    }

    if (sinkMode.contains(RowKind.UPDATE_BEFORE)) {
      return Optional.empty();
    }

    return inputMode.containsOnly(RowKind.INSERT) ? Optional.empty() : fallback;
  }

  private void requireConflictClause(StreamPhysicalSink sink, StreamOptimizeContext context) {
    if (!requireOnConflict
        || sink.conflictStrategy() != null
        || materializeMode(context) != UpsertMaterialize.AUTO
        || sink.contextResolvedTable().getResolvedSchema().getPrimaryKeyIndexes().length == 0) {
      return;
    }

    var sinkMode = sink.tableSink().getChangelogMode(inferredInputMode(sink));
    if (sinkMode.containsOnly(RowKind.INSERT)
        || sinkMode.contains(RowKind.UPDATE_BEFORE)
        || sink.primaryKeysContainsUpsertKey()) {
      return;
    }

    throw new ValidationException(
        "The query has an upsert key that differs from the primary key of the sink table '"
            + sink.contextResolvedTable().getIdentifier().asSummaryString()
            + "'. Primary key: "
            + sink.getPrimaryKeyNames()
            + ", upsert key: "
            + sink.getUpsertKeyNames()
            + ". This can lead to non-deterministic results when multiple records with different "
            + "upsert keys map to the same primary key. "
            + "Please specify an ON CONFLICT clause to define how conflicts should be handled: "
            + "ON CONFLICT DO DEDUPLICATE (update to the latest record, state intensive, since we"
            + " need to keep the entire history), or "
            + "ON CONFLICT DO ERROR (fail on conflict), or "
            + "ON CONFLICT DO NOTHING (keep first record).");
  }

  private static void validateUpsertSink(StreamPhysicalSink sink) {
    var sinkMode = sink.tableSink().getChangelogMode(inferredInputMode(sink));
    String reason;
    if (sinkMode.containsOnly(RowKind.INSERT)) {
      reason = "it only accepts INSERT (append-only) changes";
    } else if (sinkMode.contains(RowKind.UPDATE_BEFORE)) {
      reason = "it requires UPDATE_BEFORE (retract mode)";
    } else {
      return;
    }

    throw new ValidationException(
        "ON CONFLICT clause is only allowed for upsert sinks. The sink '"
            + sink.contextResolvedTable().getIdentifier().asSummaryString()
            + "' is not an upsert sink because "
            + reason
            + ".");
  }

  private static void validateSourcesHaveWatermarks(
      StreamPhysicalSink sink, InsertConflictStrategy strategy) {

    var sourcesWithoutWatermarks = new ArrayList<String>();
    collectSourcesWithoutWatermarks(sink.getInput(), sourcesWithoutWatermarks);
    if (sourcesWithoutWatermarks.isEmpty()) {
      return;
    }

    throw new ValidationException(
        "ON CONFLICT DO "
            + strategy.getBehavior()
            + " requires all source tables to define watermarks, but the following source(s) do"
            + " not: "
            + String.join(", ", sourcesWithoutWatermarks)
            + ". Please add a WATERMARK declaration to these tables.");
  }

  private static void collectSourcesWithoutWatermarks(RelNode rel, List<String> result) {
    if (rel instanceof StreamPhysicalTableSourceScan scan) {
      var table = scan.getTable().unwrap(TableSourceTable.class);
      if (table != null
          && table.contextResolvedTable().getResolvedSchema().getWatermarkSpecs().isEmpty()) {
        result.add(table.contextResolvedTable().getIdentifier().asSummaryString());
      }
      return;
    }

    rel.getInputs().forEach(input -> collectSourcesWithoutWatermarks(input, result));
  }

  private static ChangelogMode inferredInputMode(StreamPhysicalSink sink) {
    return sink.getInput()
        .getTraitSet()
        .getTrait(ModifyKindSetTraitDef.INSTANCE())
        .modifyKindSet()
        .toChangelogModeBuilder()
        .build();
  }

  private static UpsertMaterialize materializeMode(StreamOptimizeContext context) {
    return context.getTableConfig().get(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE);
  }

  private static Optional<ChangelogMode> inputChangelogMode(StreamPhysicalSink sink) {
    if (sink.getInput() instanceof StreamPhysicalRel input) {
      var changelogMode = ChangelogPlanUtils.getChangelogMode(input);
      if (changelogMode.isDefined()) {
        return Optional.of(changelogMode.get());
      }
    }

    return Optional.empty();
  }

  private static InsertConflictStrategy toConflictStrategy(SqlInsertConflictBehavior behavior) {
    return switch (behavior) {
      case ERROR -> InsertConflictStrategy.error();
      case NOTHING -> InsertConflictStrategy.nothing();
      case DEDUPLICATE -> InsertConflictStrategy.deduplicate();
    };
  }
}
