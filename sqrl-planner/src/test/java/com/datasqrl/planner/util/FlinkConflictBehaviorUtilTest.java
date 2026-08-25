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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.RelNodeAnalysis;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class FlinkConflictBehaviorUtilTest {

  @Test
  void givenAppendOnlySink_whenResolveInsertConflictBehavior_thenOmitsClause() {
    var table = table(TableType.STATIC, Optional.of(ConflictBehavior.DEDUPLICATE));

    assertThat(FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(table, false)).isEmpty();
  }

  @Test
  void givenExplicitBehaviorAndUpsertSink_whenResolveInsertConflictBehavior_thenMapsBehavior() {
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(
                table(TableType.RELATION, Optional.of(ConflictBehavior.ERROR)), true))
        .contains(SqlInsertConflictBehavior.ERROR);
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(
                table(TableType.RELATION, Optional.of(ConflictBehavior.NOTHING)), true))
        .contains(SqlInsertConflictBehavior.NOTHING);
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(
                table(TableType.RELATION, Optional.of(ConflictBehavior.DEDUPLICATE)), true))
        .contains(SqlInsertConflictBehavior.DEDUPLICATE);
  }

  @Test
  void givenStateOrStaticTableAndUpsertSink_whenResolveInsertConflictBehavior_thenDeduplicates() {
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(table(TableType.STATE), true))
        .contains(SqlInsertConflictBehavior.DEDUPLICATE);
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(
                table(TableType.VERSIONED_STATE), true))
        .contains(SqlInsertConflictBehavior.DEDUPLICATE);
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(table(TableType.STATIC), true))
        .contains(SqlInsertConflictBehavior.DEDUPLICATE);
  }

  @Test
  void
      givenStreamWithoutWatermarkAndUpsertSink_whenResolveInsertConflictBehavior_thenDeduplicates() {
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(table(TableType.STREAM), true))
        .contains(SqlInsertConflictBehavior.DEDUPLICATE);
  }

  @Test
  void givenRelationWithoutExplicitBehavior_whenResolveInsertConflictBehavior_thenOmitsClause() {
    assertThat(
            FlinkConflictBehaviorUtil.resolveInsertConflictBehavior(
                table(TableType.RELATION), true))
        .isEmpty();
  }

  @Test
  void
      givenTwoInputsWithExplicitBehaviors_whenCombineInsertConflictBehaviors_thenUsesLeftBehavior() {
    var left = relationalInput(Optional.of(ConflictBehavior.DEDUPLICATE));
    var right = relationalInput(Optional.of(ConflictBehavior.NOTHING));

    assertThat(FlinkConflictBehaviorUtil.combineInsertConflictBehaviors(left, right))
        .contains(ConflictBehavior.DEDUPLICATE);
  }

  @Test
  void givenTwoInputsWithoutExplicitBehavior_whenCombineInsertConflictBehaviors_thenReturnsEmpty() {
    var left = relationalInput(Optional.empty());
    var right = relationalInput(Optional.empty());

    assertThat(FlinkConflictBehaviorUtil.combineInsertConflictBehaviors(left, right)).isEmpty();
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

  private static RelNodeAnalysis relationalInput(
      Optional<ConflictBehavior> insertConflictBehavior) {
    return RelNodeAnalysis.builder()
        .relNode(mock(RelNode.class))
        .insertConflictBehavior(insertConflictBehavior)
        .build();
  }
}
