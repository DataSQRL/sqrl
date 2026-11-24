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

import com.datasqrl.function.IndexableFunction;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.plan.index.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.plan.table.TableStatistic;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class FilterColumnTrimmerTest {

  private static final int EMAIL = 0;
  private static final int GENDER = 1;
  private static final int STATUS = 2;
  private static final int COUNTRY = 3;
  private static final int CITY = 4;
  private static final int SIGNUP_TIME = 5;
  private static final int MIN_DISTINCT_COUNT = 20;
  private static final double ROW_COUNT = 1e6;

  private final FilterColumnTrimmer filterColumnTrimmer =
      new FilterColumnTrimmer(MIN_DISTINCT_COUNT);

  @Test
  void givenEqualityColumnWithFewDistinctValues_whenTrim_thenRemovesTheColumn() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0));
    var emailAndGender = equalityOn(customers, EMAIL, GENDER);

    assertThat(filterColumnTrimmer.trim(emailAndGender))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  @Test
  void givenEqualityColumnWithMinDistinctCount_whenTrim_thenKeepsTheColumn() {
    var customers = customersWith(Map.of(Set.of(STATUS), (double) MIN_DISTINCT_COUNT));
    var emailAndStatus = equalityOn(customers, EMAIL, STATUS);

    assertThat(filterColumnTrimmer.trim(emailAndStatus)).containsSame(emailAndStatus);
  }

  @Test
  void givenNoDistinctCounts_whenTrim_thenKeepsEveryColumn() {
    var customers = customersWith(Map.of());
    var emailAndGender = equalityOn(customers, EMAIL, GENDER);

    assertThat(filterColumnTrimmer.trim(emailAndGender)).containsSame(emailAndGender);
  }

  @Test
  void givenOnlyEqualityColumnsWithFewDistinctValues_whenTrim_thenNothingIsLeftToIndex() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0, Set.of(STATUS), 6.0));
    var genderAndStatus = equalityOn(customers, GENDER, STATUS);

    assertThat(filterColumnTrimmer.trim(genderAndStatus)).isEmpty();
  }

  @Test
  void givenColumnCombinationWithFewDistinctValues_whenTrim_thenRemovesEachOfItsColumns() {
    var customers = customersWith(Map.of(Set.of(GENDER, STATUS), 12.0));
    var emailAndGender = equalityOn(customers, EMAIL, GENDER);

    assertThat(filterColumnTrimmer.trim(emailAndGender))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  /** The inequality column can be the sort of a query with a limit, which an index serves. */
  @Test
  void givenInequalityColumnWithFewDistinctValues_whenTrim_thenKeepsTheColumn() {
    var customers = customersWith(Map.of(Set.of(STATUS), 6.0));
    var sortedByStatus = new QueryIndexSummary(customers, Set.of(), Set.of(STATUS), Set.of(), 1.0);

    assertThat(filterColumnTrimmer.trim(sortedByStatus)).containsSame(sortedByStatus);
  }

  @Test
  void givenTrimmedEqualityColumnBesideOtherFilters_whenTrim_thenKeepsTheOtherFilters() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0));
    var textSearch = new IndexableFunctionCall(List.of(EMAIL), mock(IndexableFunction.class));
    var genderWithOtherFilters =
        new QueryIndexSummary(customers, Set.of(GENDER), Set.of(STATUS), Set.of(textSearch), 3.0);

    assertThat(filterColumnTrimmer.trim(genderWithOtherFilters))
        .hasValueSatisfying(
            trimmed -> {
              assertThat(trimmed.getEqualityColumns()).isEmpty();
              assertThat(trimmed.getInequalityColumns()).containsExactly(STATUS);
              assertThat(trimmed.getFunctionCalls()).containsExactly(textSearch);
              assertThat(trimmed.getCount()).isEqualTo(3.0);
            });
  }

  @Test
  void givenMinDistinctCountOfZero_whenTrim_thenKeepsEveryColumn() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0));
    var gender = equalityOn(customers, GENDER);

    assertThat(new FilterColumnTrimmer(0).trim(gender)).containsSame(gender);
  }

  @Test
  void givenUniqueEqualityColumn_whenTrim_thenRemovesEveryOtherColumn() {
    var customers = customersWith(Map.of(Set.of(EMAIL), ROW_COUNT));
    var emailCountryAndSignupTime =
        new QueryIndexSummary(
            customers, Set.of(EMAIL, COUNTRY), Set.of(SIGNUP_TIME), Set.of(), 1.0);

    assertThat(filterColumnTrimmer.trim(emailCountryAndSignupTime))
        .hasValueSatisfying(
            trimmed -> {
              assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL);
              assertThat(trimmed.getInequalityColumns()).isEmpty();
            });
  }

  @Test
  void givenUniqueColumnCombination_whenTrim_thenKeepsOnlyItsColumns() {
    var customers = customersWith(Map.of(Set.of(COUNTRY, CITY), ROW_COUNT));
    var countryCityAndStatus = equalityOn(customers, COUNTRY, CITY, STATUS);

    assertThat(filterColumnTrimmer.trim(countryCityAndStatus))
        .hasValueSatisfying(
            trimmed ->
                assertThat(trimmed.getEqualityColumns()).containsExactlyInAnyOrder(COUNTRY, CITY));
  }

  @Test
  void givenUniqueColumnCombinationFilteredInPart_whenTrim_thenKeepsEveryColumn() {
    var customers = customersWith(Map.of(Set.of(COUNTRY, CITY), ROW_COUNT));
    var countryAndStatus = equalityOn(customers, COUNTRY, STATUS);

    assertThat(filterColumnTrimmer.trim(countryAndStatus)).containsSame(countryAndStatus);
  }

  @Test
  void givenAlmostUniqueColumn_whenTrim_thenKeepsEveryColumn() {
    var customers = customersWith(Map.of(Set.of(EMAIL), ROW_COUNT - 1));
    var emailAndCountry = equalityOn(customers, EMAIL, COUNTRY);

    assertThat(filterColumnTrimmer.trim(emailAndCountry)).containsSame(emailAndCountry);
  }

  @Test
  void givenSeveralUniqueColumnSets_whenTrim_thenKeepsTheOneWithFewestColumns() {
    var customers =
        customersWith(Map.of(Set.of(COUNTRY, CITY), ROW_COUNT, Set.of(EMAIL), ROW_COUNT));
    var emailCountryAndCity = equalityOn(customers, EMAIL, COUNTRY, CITY);

    assertThat(filterColumnTrimmer.trim(emailCountryAndCity))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  @Test
  void givenUniqueColumnOnlyByEstimatedRowCount_whenTrim_thenKeepsEveryColumn() {
    var estimatedStatistic =
        TableStatistic.fromEstimate(ROW_COUNT).withDistinctCounts(Map.of(Set.of(EMAIL), ROW_COUNT));
    var customers = customersWith(estimatedStatistic);
    var emailAndCountry = equalityOn(customers, EMAIL, COUNTRY);

    assertThat(filterColumnTrimmer.trim(emailAndCountry)).containsSame(emailAndCountry);
  }

  @Test
  void givenUniqueColumnAndUnknownRowCount_whenTrim_thenKeepsEveryColumn() {
    var unknownStatistic =
        TableStatistic.UNKNOWN.withDistinctCounts(Map.of(Set.of(EMAIL), ROW_COUNT));
    var customers = customersWith(unknownStatistic);
    var emailAndCountry = equalityOn(customers, EMAIL, COUNTRY);

    assertThat(filterColumnTrimmer.trim(emailAndCountry)).containsSame(emailAndCountry);
  }

  @Test
  void givenUniqueColumnCombinationWithColumnOfFewDistinctValues_whenTrim_thenRemovesThatColumn() {
    var customers = customersWith(Map.of(Set.of(EMAIL, GENDER), ROW_COUNT, Set.of(GENDER), 2.0));
    var emailGenderAndCountry = equalityOn(customers, EMAIL, GENDER, COUNTRY);

    assertThat(filterColumnTrimmer.trim(emailGenderAndCountry))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  @Test
  void givenColumnThatAddsFewDistinctValues_whenTrim_thenRemovesTheColumn() {
    var customers = customersWith(Map.of(Set.of(COUNTRY), 200.0, Set.of(COUNTRY, CITY), 2e3));
    var countryAndCity = equalityOn(customers, COUNTRY, CITY);

    assertThat(filterColumnTrimmer.trim(countryAndCity))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(COUNTRY));
  }

  @Test
  void givenColumnThatAddsMinDistinctCountTimesTheDistinctValues_whenTrim_thenKeepsTheColumn() {
    var customers =
        customersWith(
            Map.of(Set.of(COUNTRY), 200.0, Set.of(COUNTRY, CITY), 200.0 * MIN_DISTINCT_COUNT));
    var countryAndCity = equalityOn(customers, COUNTRY, CITY);

    assertThat(filterColumnTrimmer.trim(countryAndCity)).containsSame(countryAndCity);
  }

  @Test
  void givenColumnThatAddsFewDistinctValuesToUnfilteredColumn_whenTrim_thenKeepsTheColumn() {
    var customers = customersWith(Map.of(Set.of(COUNTRY), 200.0, Set.of(COUNTRY, CITY), 2e3));
    var emailAndCity = equalityOn(customers, EMAIL, CITY);

    assertThat(filterColumnTrimmer.trim(emailAndCity)).containsSame(emailAndCity);
  }

  @Test
  void givenTwoColumnsThatAddFewDistinctValuesToEachOther_whenTrim_thenKeepsOneOfThem() {
    var customers =
        customersWith(Map.of(Set.of(EMAIL), 1e3, Set.of(CITY), 1e3, Set.of(EMAIL, CITY), 1.2e3));
    var emailAndCity = equalityOn(customers, EMAIL, CITY);

    assertThat(filterColumnTrimmer.trim(emailAndCity))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  /** The gender column is removed itself, so the country column has nothing to add little to. */
  @Test
  void givenColumnThatAddsFewDistinctValuesToRemovedColumn_whenTrim_thenKeepsTheColumn() {
    var customers = customersWith(Map.of(Set.of(GENDER), 10.0, Set.of(GENDER, COUNTRY), 150.0));
    var genderAndCountry = equalityOn(customers, GENDER, COUNTRY);

    assertThat(filterColumnTrimmer.trim(genderAndCountry))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(COUNTRY));
  }

  /** The city column is removed for the email column, so the email column must stay. */
  @Test
  void givenColumnThatAnotherRemovalWasComparedWith_whenTrim_thenKeepsTheColumn() {
    var customers =
        customersWith(
            Map.of(
                Set.of(EMAIL), 1e3,
                Set.of(EMAIL, CITY), 1.2e3,
                Set.of(COUNTRY), 1e5,
                Set.of(EMAIL, COUNTRY), 1.5e5));
    var emailCityAndCountry = equalityOn(customers, EMAIL, CITY, COUNTRY);

    assertThat(filterColumnTrimmer.trim(emailCityAndCountry))
        .hasValueSatisfying(
            trimmed ->
                assertThat(trimmed.getEqualityColumns()).containsExactlyInAnyOrder(EMAIL, COUNTRY));
  }

  private static QueryIndexSummary equalityOn(NamedTable table, Integer... equalityColumns) {
    return new QueryIndexSummary(table, Set.of(equalityColumns), Set.of(), Set.of(), 1.0);
  }

  private static NamedTable customersWith(Map<Set<Integer>, Double> distinctCounts) {
    return customersWith(TableStatistic.fromHint(ROW_COUNT).withDistinctCounts(distinctCounts));
  }

  private static NamedTable customersWith(TableStatistic tableStatistic) {
    var relNode = mock(RelNode.class);
    var analysis =
        TableAnalysis.builder()
            .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "Customers"))
            .collapsedRelnode(relNode)
            .originalRelnode(relNode)
            .tableStatistic(tableStatistic)
            .build();
    return new NamedTable("Customers", "Customers", analysis, null);
  }
}
