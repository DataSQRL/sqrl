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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class ColumnNamesHintTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(ColumnNamesHintTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType BIGINT = TYPES.createSqlType(SqlTypeName.BIGINT);

  private static final RelDataType ROW_TYPE =
      TYPES.builder().add("customerId", BIGINT).add("cnt", BIGINT).add("CNT", BIGINT).build();

  @Test
  void givenPlainNameMatchingOneColumn_whenValidateAndUpdate_thenResolvesItIgnoringCase() {
    var hint = hint("primary_key(CUSTOMERID)");

    hint.validateAndUpdate(ROW_TYPE);

    assertThat(hint.getColumnNames()).containsExactly("customerId");
    assertThat(hint.getColumnIndexes()).containsExactly(0);
  }

  @Test
  void givenNamesInBackticks_whenValidateAndUpdate_thenResolvesTheExactNames() {
    var hint = hint("index(btree, `CNT` DESC, `cnt`)");

    hint.validateAndUpdate(ROW_TYPE);

    assertThat(hint.getColumnNames()).containsExactly("CNT", "cnt");
    assertThat(hint.getColumnIndexes()).containsExactly(2, 1);
  }

  @Test
  void givenAmbiguousNameWithBacktick_whenValidateAndUpdateWithSuggestedName_thenResolvesIt() {
    var rowType = TYPES.builder().add("it`s", BIGINT).add("IT`S", BIGINT).build();

    assertThatThrownBy(() -> hint("primary_key(it`s)").validateAndUpdate(rowType))
        .hasMessageEndingWith("write `it``s` or `IT``S` to match exactly one");

    var hint = hint("index(btree, `IT``S` DESC)");
    hint.validateAndUpdate(rowType);

    assertThat(hint.getColumnNames()).containsExactly("IT`S");
  }

  @Test
  void givenResolvedHint_whenValidateAndUpdateAgain_thenResolvesTheWrittenNames() {
    var hint = hint("query_by_all(`CNT`)");
    hint.validateAndUpdate(ROW_TYPE);

    hint.validateAndUpdate(ROW_TYPE);

    assertThat(hint.getColumnIndexes()).containsExactly(2);
  }

  @Test
  void givenPlainNameMatchingSeveralColumns_whenValidateAndUpdate_thenThrows() {
    var hint = hint("index(btree, CNT)");

    assertThatThrownBy(() -> hint.validateAndUpdate(ROW_TYPE))
        .isInstanceOf(StatementParserException.class)
        .hasMessage(
            "index hint references column [CNT] which matches columns that differ only in case:"
                + " write `cnt` or `CNT` to match exactly one");
  }

  @Test
  void givenNameInBackticksWithOtherCase_whenValidateAndUpdate_thenThrowsMissingColumn() {
    var hint = hint("partition_key(`CUSTOMERID`)");

    assertThatThrownBy(() -> hint.validateAndUpdate(ROW_TYPE))
        .isInstanceOf(StatementParserException.class)
        .hasMessage(
            "partition_key hint reference column [`CUSTOMERID`] that does not exist in table");
  }

  private static ColumnNamesHint hint(String hint) {
    var parsed = SqrlHint.parse(new ParsedObject<>(hint, FileLocation.START)).get(0);
    return (ColumnNamesHint) PlannerHint.from(parsed, ErrorCollector.root()).orElseThrow();
  }
}
