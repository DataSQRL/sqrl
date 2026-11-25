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
package com.datasqrl.deployment.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class RowTypeTextTest {

  @Test
  void givenRowType_whenColumnTypes_thenTypesWithoutNames() {
    assertThat(RowTypeText.columnTypes("ROW<`id` INT, `cnt` BIGINT NOT NULL>"))
        .hasValue(List.of("INT", "BIGINT NOT NULL"));
  }

  @Test
  void givenTypeWithCommaInParentheses_whenColumnTypes_thenOneColumn() {
    assertThat(RowTypeText.columnTypes("ROW<`price` DECIMAL(10, 2), `id` INT>"))
        .hasValue(List.of("DECIMAL(10, 2)", "INT"));
  }

  @Test
  void givenNestedRow_whenColumnTypes_thenNestedTypeKeepsItsNames() {
    assertThat(RowTypeText.columnTypes("ROW<`id` INT, `payload` ROW<`a` INT, `b` STRING>>"))
        .hasValue(List.of("INT", "ROW<`a` INT, `b` STRING>"));
  }

  @Test
  void givenNameWithCommaAndBacktick_whenColumnTypes_thenNameRemoved() {
    assertThat(RowTypeText.columnTypes("ROW<`a, ``b` INT, `c` BIGINT>"))
        .hasValue(List.of("INT", "BIGINT"));
  }

  @Test
  void givenColumnDescription_whenColumnTypes_thenDescriptionRemoved() {
    assertThat(RowTypeText.columnTypes("ROW<`id` INT 'the id, unique', `cnt` BIGINT>"))
        .hasValue(List.of("INT", "BIGINT"));
  }

  @Test
  void givenNotNullRow_whenColumnTypes_thenTypesOfItsColumns() {
    assertThat(RowTypeText.columnTypes("ROW<`id` INT> NOT NULL")).hasValue(List.of("INT"));
  }

  @Test
  void givenTypeThatIsNotRow_whenColumnTypes_thenEmpty() {
    assertThat(RowTypeText.columnTypes("BIGINT")).isEmpty();
  }
}
