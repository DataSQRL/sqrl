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
package com.datasqrl.planner.hint;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class RowCountHintTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(RowCountHintTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType STRING = TYPES.createSqlType(SqlTypeName.VARCHAR);

  private static final RelDataType ROW_TYPE =
      TYPES.builder().add("email", STRING).add("country", STRING).add("city", STRING).build();

  @Test
  void givenHintWithoutColumns_whenIsTableRowCount_thenTrue() {
    assertThat(rowCountHint("row_count(1e6)").isTableRowCount()).isTrue();
  }

  @Test
  void givenHintWithColumns_whenIsTableRowCount_thenFalse() {
    assertThat(rowCountHint("row_count(email, 1e6)").isTableRowCount()).isFalse();
  }

  @Test
  void givenHintsWithColumns_whenDistinctCounts_thenCountsByColumnIndexes() {
    var rowCountHints =
        Stream.of(
            rowCountHint("row_count(email, 1e6)"), rowCountHint("row_count(city, country, 2e3)"));

    assertThat(RowCountHint.distinctCounts(rowCountHints))
        .isEqualTo(Map.of(Set.of(0), 1e6, Set.of(1, 2), 2e3));
  }

  @Test
  void givenHintWithoutColumns_whenDistinctCounts_thenLeavesItOut() {
    var rowCountHints = Stream.of(rowCountHint("row_count(1e6)"));

    assertThat(RowCountHint.distinctCounts(rowCountHints)).isEmpty();
  }

  @Test
  void givenTwoHintsForTheSameColumns_whenDistinctCounts_thenFirstHintCounts() {
    var rowCountHints =
        Stream.of(
            rowCountHint("row_count(country, city, 2e3)"),
            rowCountHint("row_count(city, country, 5e3)"));

    assertThat(RowCountHint.distinctCounts(rowCountHints)).isEqualTo(Map.of(Set.of(1, 2), 2e3));
  }

  private static RowCountHint rowCountHint(String hint) {
    var parsed = SqrlHint.parse(new ParsedObject<>(hint, FileLocation.START)).get(0);
    var rowCountHint = (RowCountHint) PlannerHint.from(parsed, ErrorCollector.root()).orElseThrow();
    rowCountHint.validateAndUpdate(ROW_TYPE);
    return rowCountHint;
  }
}
