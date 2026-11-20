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
package com.datasqrl.plan.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;

import com.datasqrl.plan.index.QueryIndexSummary;
import com.datasqrl.plan.table.TableStatistic;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class SqrlRelMdSelectivityTest {

  private static final int EMAIL = 0;
  private static final int COUNTRY = 1;
  private static final int CITY = 2;
  private static final int ZIP = 3;
  private static final int SIGNUP_TIME = 4;

  private static final Offset<Double> ROUNDING = within(1e-15);

  @Test
  void givenNoDistinctCounts_whenGetSelectivity_thenEachEqualityColumnGetsTheDefault() {
    var table = tableWithDistinctCounts(Map.of());

    assertThat(selectivity(table, Set.of(EMAIL, COUNTRY), Set.of()))
        .isCloseTo(0.05 * 0.05, ROUNDING);
  }

  @Test
  void givenDistinctCountOfColumn_whenGetSelectivity_thenEqualityMatchesOneInThatMany() {
    var table = tableWithDistinctCounts(Map.of(Set.of(EMAIL), 1e6));

    assertThat(selectivity(table, Set.of(EMAIL), Set.of())).isCloseTo(1e-6, ROUNDING);
  }

  @Test
  void givenDistinctCountOfOneColumn_whenGetSelectivity_thenOtherColumnsGetTheDefault() {
    var table = tableWithDistinctCounts(Map.of(Set.of(COUNTRY), 200d));

    assertThat(selectivity(table, Set.of(COUNTRY, ZIP), Set.of()))
        .isCloseTo(1 / 200d * 0.05, ROUNDING);
  }

  @Test
  void givenDistinctCountOfColumnPair_whenEqualityOnBothColumns_thenDistinctCountApplies() {
    var table = tableWithDistinctCounts(Map.of(Set.of(COUNTRY, CITY), 2e3));

    assertThat(selectivity(table, Set.of(COUNTRY, CITY), Set.of())).isCloseTo(1 / 2e3, ROUNDING);
  }

  @Test
  void givenDistinctCountOfColumnPair_whenEqualityOnOneColumn_thenColumnGetsTheDefault() {
    var table = tableWithDistinctCounts(Map.of(Set.of(COUNTRY, CITY), 2e3));

    assertThat(selectivity(table, Set.of(COUNTRY), Set.of())).isCloseTo(0.05, ROUNDING);
  }

  @Test
  void givenDistinctCountsThatShareAColumn_whenGetSelectivity_thenTheOneWithMoreColumnsApplies() {
    var table = tableWithDistinctCounts(Map.of(Set.of(COUNTRY, CITY), 2e3, Set.of(COUNTRY), 200d));

    assertThat(selectivity(table, Set.of(COUNTRY, CITY), Set.of())).isCloseTo(1 / 2e3, ROUNDING);
  }

  @Test
  void givenDistinctCountsOfSeparateColumns_whenGetSelectivity_thenBothApply() {
    var table = tableWithDistinctCounts(Map.of(Set.of(COUNTRY), 200d, Set.of(ZIP), 4e4));

    assertThat(selectivity(table, Set.of(COUNTRY, ZIP), Set.of()))
        .isCloseTo(1 / 200d / 4e4, ROUNDING);
  }

  @Test
  void givenDistinctCountOfColumn_whenInequalityOnThatColumn_thenInequalityGetsTheDefault() {
    var table = tableWithDistinctCounts(Map.of(Set.of(SIGNUP_TIME), 1e6));

    assertThat(selectivity(table, Set.of(), Set.of(SIGNUP_TIME))).isCloseTo(0.5, ROUNDING);
  }

  @Test
  void givenDistinctCountBelowOne_whenGetSelectivity_thenSelectivityIsAtMostOne() {
    var table = tableWithDistinctCounts(Map.of(Set.of(COUNTRY), 0.5));

    assertThat(selectivity(table, Set.of(COUNTRY), Set.of())).isEqualTo(1.0);
  }

  private static double selectivity(
      TableAnalysis table, Set<Integer> equalityColumns, Set<Integer> inequalityColumns) {
    var constraints =
        new QueryIndexSummary(null, equalityColumns, inequalityColumns, Set.of(), 1.0);
    return SqrlRelMdSelectivity.getSelectivity(table, constraints);
  }

  private static TableAnalysis tableWithDistinctCounts(Map<Set<Integer>, Double> distinctCounts) {
    var relNode = mock(RelNode.class);
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "Customers"))
        .collapsedRelnode(relNode)
        .originalRelnode(relNode)
        .tableStatistic(TableStatistic.fromHint(1e6).withDistinctCounts(distinctCounts))
        .build();
  }
}
