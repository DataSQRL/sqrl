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
package com.datasqrl.planner.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class ColumnReferenceTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(ColumnReferenceTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType BIGINT = TYPES.createSqlType(SqlTypeName.BIGINT);

  private static final RelDataType ROW_TYPE =
      TYPES.builder().add("customerId", BIGINT).add("cnt", BIGINT).add("CNT", BIGINT).build();

  @Test
  void givenPlainName_whenParse_thenIsNotQuoted() {
    assertThat(ColumnReference.parse("customerid"))
        .isEqualTo(new ColumnReference("customerid", false));
  }

  @Test
  void givenNameInBackticks_whenParse_thenIsQuotedWithoutTheBackticks() {
    assertThat(ColumnReference.parse("`customer id`"))
        .isEqualTo(new ColumnReference("customer id", true));
    assertThat(ColumnReference.parse("`it``s`")).isEqualTo(new ColumnReference("it`s", true));
  }

  @Test
  void givenMisplacedBackticks_whenParse_thenIsPlainNameWithTheBackticks() {
    assertThat(ColumnReference.parse("`cnt")).isEqualTo(new ColumnReference("`cnt", false));
    assertThat(ColumnReference.parse("`my`col`")).isEqualTo(new ColumnReference("`my`col`", false));
    assertThat(ColumnReference.parse("``")).isEqualTo(new ColumnReference("``", false));
  }

  @Test
  void givenPlainName_whenMatchingColumns_thenMatchesColumnsIgnoringCase() {
    assertThat(names(new ColumnReference("CUSTOMERID", false))).containsExactly("customerId");
    assertThat(names(new ColumnReference("Cnt", false))).containsExactly("cnt", "CNT");
    assertThat(names(new ColumnReference("orders", false))).isEmpty();
  }

  @Test
  void givenNameInBackticks_whenMatchingColumns_thenMatchesOnlyTheExactName() {
    assertThat(names(new ColumnReference("CNT", true))).containsExactly("CNT");
    assertThat(names(new ColumnReference("CUSTOMERID", true))).isEmpty();
  }

  @Test
  void givenTwoNames_whenOverlaps_thenComparesQuotedNamesExactly() {
    var plain = new ColumnReference("cnt", false);
    var quotedLower = new ColumnReference("cnt", true);
    var quotedUpper = new ColumnReference("CNT", true);

    assertThat(plain.overlaps(new ColumnReference("CNT", false))).isTrue();
    assertThat(plain.overlaps(quotedUpper)).isTrue();
    assertThat(quotedUpper.overlaps(plain)).isTrue();
    assertThat(quotedLower.overlaps(quotedUpper)).isFalse();
    assertThat(quotedLower.overlaps(new ColumnReference("cnt", true))).isTrue();
    assertThat(plain.overlaps(new ColumnReference("count", false))).isFalse();
  }

  @Test
  void givenAmbiguousMatches_whenAlternatives_thenListsTheNamesInBackticks() {
    var matches = new ColumnReference("cnt", false).matchingColumns(ROW_TYPE);

    assertThat(ColumnReference.alternatives(matches)).isEqualTo("`cnt` or `CNT`");
  }

  @Test
  void givenReference_whenToString_thenIsWrittenForm() {
    assertThat(new ColumnReference("cnt", false)).hasToString("cnt");
    assertThat(new ColumnReference("CNT", true)).hasToString("`CNT`");
    assertThat(new ColumnReference("it`s", true)).hasToString("`it``s`");
  }

  private static List<String> names(ColumnReference reference) {
    return reference.matchingColumns(ROW_TYPE).stream().map(RelDataTypeField::getName).toList();
  }
}
