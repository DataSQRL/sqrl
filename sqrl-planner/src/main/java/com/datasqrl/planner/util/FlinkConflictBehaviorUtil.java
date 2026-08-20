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
package com.datasqrl.planner.util;

import com.datasqrl.planner.analyzer.RelNodeAnalysis;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import java.util.List;
import java.util.Optional;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.table.api.InsertConflictStrategy;

/**
 * Propagates explicit Flink {@code ON CONFLICT} behaviors and resolves final behavior for generated
 * sink inserts.
 */
public class FlinkConflictBehaviorUtil {

  /**
   * Resolves the {@code ON CONFLICT} behavior for an insert into a Flink sink.
   *
   * <p>Append-only sinks cannot accept an {@code ON CONFLICT} clause. For upsert sinks, an explicit
   * behavior recorded during relational analysis takes precedence. Otherwise, state, versioned
   * state, and static tables deduplicate, while streams use {@code NOTHING} only when every leaf
   * source has a watermark and deduplicate in all other cases.
   *
   * @param table analysis of the table being inserted
   * @param isUpsertSink whether the resolved Flink sink accepts updates
   * @return the behavior to render in an {@code ON CONFLICT} clause, or empty for no clause
   */
  public static Optional<SqlInsertConflictBehavior> resolveInsertConflictBehavior(
      TableAnalysis table, boolean isUpsertSink) {

    if (!isUpsertSink) {
      return Optional.empty();
    }

    if (table.getInsertConflictBehavior().isPresent()) {
      return table
          .getInsertConflictBehavior()
          .map(FlinkConflictBehaviorUtil::toSqlConflictBehavior);
    }

    return switch (table.getType()) {
      case VERSIONED_STATE, STATE, STATIC -> Optional.of(SqlInsertConflictBehavior.DEDUPLICATE);
      case STREAM ->
          Optional.of(
              hasSourceWatermarks(table)
                  ? SqlInsertConflictBehavior.NOTHING
                  : SqlInsertConflictBehavior.DEDUPLICATE);
      default -> Optional.empty();
    };
  }

  /**
   * Combines the explicit insert-conflict behaviors of two relational inputs.
   *
   * <p>The first explicit behavior in left-to-right input order is propagated. The current planner
   * only records {@code DEDUPLICATE} as an explicit behavior, so combining inputs cannot introduce
   * conflicting policies.
   *
   * @param left analysis of the left input
   * @param right analysis of the right input
   * @return the first explicit behavior, or empty when neither input specifies one
   */
  public static Optional<InsertConflictStrategy.ConflictBehavior> combineInsertConflictBehaviors(
      RelNodeAnalysis left, RelNodeAnalysis right) {
    return combineInsertConflictBehaviors(List.of(left, right));
  }

  /**
   * Combines the explicit insert-conflict behaviors of relational inputs.
   *
   * <p>The first explicit behavior in input order is propagated so that compound relational
   * operators retain the conflict policy introduced by one of their inputs.
   *
   * @param inputs analyses of the operator inputs in relational input order
   * @return the first explicit behavior, or empty when no input specifies one
   */
  public static Optional<InsertConflictStrategy.ConflictBehavior> combineInsertConflictBehaviors(
      List<RelNodeAnalysis> inputs) {
    return inputs.stream()
        .map(RelNodeAnalysis::getInsertConflictBehavior)
        .flatMap(Optional::stream)
        .findFirst();
  }

  private static SqlInsertConflictBehavior toSqlConflictBehavior(
      InsertConflictStrategy.ConflictBehavior behavior) {
    return switch (behavior) {
      case ERROR -> SqlInsertConflictBehavior.ERROR;
      case NOTHING -> SqlInsertConflictBehavior.NOTHING;
      case DEDUPLICATE -> SqlInsertConflictBehavior.DEDUPLICATE;
    };
  }

  private static boolean hasSourceWatermarks(TableOrFunctionAnalysis table) {
    if (table instanceof TableAnalysis tableAnalysis
        && !tableAnalysis.isSourceOrSink()
        && !tableAnalysis.getFromTables().isEmpty()) {

      return tableAnalysis.getFromTables().stream()
          .allMatch(FlinkConflictBehaviorUtil::hasSourceWatermarks);
    }

    return table.getRowTime().isPresent();
  }
}
