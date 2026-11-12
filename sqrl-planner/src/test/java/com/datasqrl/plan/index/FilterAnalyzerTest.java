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
package com.datasqrl.plan.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.plan.index.UnindexableFilter.Reason;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class FilterAnalyzerTest {

  private static final int ORDER_ID = 0;
  private static final int AGENCY_ID = 1;
  private static final int EMAIL = 2;

  private final FlinkTypeFactory typeFactory =
      new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
  private final RelDataType string =
      typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
  private final RelDataType rowType =
      typeFactory
          .builder()
          .add("order_id", bigint)
          .add("agency_id", bigint)
          .add("email", typeFactory.createSqlType(SqlTypeName.VARCHAR, 100))
          .build();
  private final NamedTable table = new NamedTable("Orders", "Orders_1", tableAnalysis(), null);

  @Test
  void givenColumnEqualsParameter_whenAnalyze_thenEqualityColumn() {
    var analysis = analyze(equals(column(AGENCY_ID), param(0, bigint)));

    assertThat(analysis.summaries())
        .singleElement()
        .satisfies(
            summary -> {
              assertThat(summary.getEqualityColumns()).containsExactly(AGENCY_ID);
              assertThat(summary.getInequalityColumns()).isEmpty();
            });
    assertThat(analysis.unindexableFilters()).isEmpty();
  }

  @Test
  void givenRangeOnColumn_whenAnalyze_thenInequalityColumn() {
    var analysis =
        analyze(
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, param(0, bigint), column(ORDER_ID)));

    assertThat(analysis.summaries())
        .singleElement()
        .satisfies(summary -> assertThat(summary.getInequalityColumns()).containsExactly(ORDER_ID));
  }

  @Test
  void givenArithmeticAndPlannerCastOnParameterSide_whenAnalyze_thenEqualityColumnWithoutWarning() {
    var parameterMinusOne =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            param(0, bigint),
            rexBuilder.makeBigintLiteral(BigDecimal.ONE));
    var analysis =
        analyze(equals(column(AGENCY_ID), rexBuilder.makeAbstractCast(bigint, parameterMinusOne)));

    assertThat(analysis.summaries())
        .singleElement()
        .satisfies(summary -> assertThat(summary.getEqualityColumns()).containsExactly(AGENCY_ID));
    assertThat(analysis.unindexableFilters()).isEmpty();
  }

  @Test
  void givenArithmeticOnColumn_whenAnalyze_thenNoSummaryAndArithmeticWarning() {
    var agencyPlusOne = plusOne(column(AGENCY_ID));
    var analysis = analyze(equals(agencyPlusOne, param(0, bigint)));

    assertThat(analysis.summaries()).isEmpty();
    assertThat(analysis.unindexableFilters())
        .singleElement()
        .satisfies(
            filter -> {
              assertThat(filter.reason()).isEqualTo(Reason.ARITHMETIC);
              assertThat(filter.expression()).isEqualTo(agencyPlusOne);
            });
  }

  @Test
  void givenCastOnColumn_whenAnalyze_thenNoSummaryAndCastWarning() {
    var analysis = analyze(equals(cast(column(AGENCY_ID), string), param(0, string)));

    assertThat(analysis.summaries()).isEmpty();
    assertThat(analysis.unindexableFilters())
        .singleElement()
        .satisfies(filter -> assertThat(filter.reason()).isEqualTo(Reason.CAST));
  }

  @Test
  void givenFunctionOnColumn_whenAnalyze_thenNoSummaryAndExpressionWarning() {
    var lowerEmail = rexBuilder.makeCall(SqlStdOperatorTable.LOWER, column(EMAIL));
    var analysis = analyze(equals(lowerEmail, param(0, string)));

    assertThat(analysis.summaries()).isEmpty();
    assertThat(analysis.unindexableFilters())
        .singleElement()
        .satisfies(
            filter -> {
              assertThat(filter.reason()).isEqualTo(Reason.EXPRESSION);
              assertThat(filter.expression()).isEqualTo(lowerEmail);
            });
  }

  @Test
  void givenCastBetweenCharacterTypesOnColumn_whenAnalyze_thenEqualityColumn() {
    var analysis = analyze(equals(cast(column(EMAIL), string), param(0, string)));

    assertThat(analysis.summaries())
        .singleElement()
        .satisfies(summary -> assertThat(summary.getEqualityColumns()).containsExactly(EMAIL));
    assertThat(analysis.unindexableFilters()).isEmpty();
  }

  @Test
  void givenNotEquals_whenAnalyze_thenNoSummaryAndNoWarning() {
    var analysis =
        analyze(
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT_EQUALS, column(AGENCY_ID), param(0, bigint)));

    assertThat(analysis.summaries()).isEmpty();
    assertThat(analysis.unindexableFilters()).isEmpty();
  }

  @Test
  void givenColumnsOnBothSides_whenAnalyze_thenNoSummaryAndNoWarning() {
    var analysis = analyze(equals(plusOne(column(AGENCY_ID)), column(ORDER_ID)));

    assertThat(analysis.summaries()).isEmpty();
    assertThat(analysis.unindexableFilters()).isEmpty();
  }

  @Test
  void givenOptionalLookup_whenAnalyze_thenSummariesWithAndWithoutLookedUpColumn() {
    var optionalLookup = new LookupParameters().optionalLookup(bigint);
    var analysis =
        analyze(
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                equals(column(AGENCY_ID), param(0, bigint)),
                equals(column(ORDER_ID), optionalLookup)));

    assertThat(analysis.summaries())
        .extracting(QueryIndexSummary::getEqualityColumns)
        .containsExactlyInAnyOrder(Set.of(ORDER_ID, AGENCY_ID), Set.of(AGENCY_ID));
  }

  @Test
  void givenArithmeticOnColumn_whenToWarning_thenNamesFilterFunctionTableAndRemedy() {
    var filter =
        analyze(equals(plusOne(column(AGENCY_ID)), param(0, bigint))).unindexableFilters().get(0);

    assertThat(filter.toWarning("ByAgency", List.of("agency")))
        .isEqualTo(
            "The filter `agency_id + 1 = :agency` in `ByAgency` cannot use an index on table"
                + " `Orders_1` because column `agency_id` is wrapped in the expression"
                + " `agency_id + 1`. Move the arithmetic to the other side of the comparison so"
                + " that `agency_id` stands alone.");
  }

  @Test
  void givenCastOnColumn_whenToWarning_thenSuggestsTheColumnType() {
    var filter =
        analyze(equals(cast(column(AGENCY_ID), string), param(0, string)))
            .unindexableFilters()
            .get(0);

    assertThat(filter.toWarning("ByAgency", List.of("agency")))
        .startsWith(
            "The filter `CAST(agency_id AS STRING) = :agency` in `ByAgency` cannot use an index")
        .contains("Compare `agency_id` with a value of its type `BIGINT`")
        .contains("/*+engine(flink) */");
  }

  private IndexAnalysis analyze(RexNode condition) {
    return FilterAnalyzer.analyze(table, condition, new SqrlRexUtil(typeFactory));
  }

  private RexNode column(int index) {
    return RexInputRef.of(index, rowType);
  }

  private RexNode param(int index, RelDataType type) {
    return rexBuilder.makeDynamicParam(type, index);
  }

  private RexNode equals(RexNode left, RexNode right) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
  }

  private RexNode plusOne(RexNode node) {
    return rexBuilder.makeCall(
        SqlStdOperatorTable.PLUS, node, rexBuilder.makeBigintLiteral(BigDecimal.ONE));
  }

  private RexNode cast(RexNode node, RelDataType type) {
    return rexBuilder.makeAbstractCast(type, node, false);
  }

  private TableAnalysis tableAnalysis() {
    var relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "Orders"))
        .collapsedRelnode(relNode)
        .originalRelnode(relNode)
        .build();
  }
}
