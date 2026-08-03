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

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.plan.global.IndexType;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.junit.jupiter.api.Test;

class IndexHintTest {

  private final IndexHint.IndexHintFactory factory = new IndexHint.IndexHintFactory();

  @Test
  void givenDescendingIndexColumn_whenCreate_thenRetainsSortDirections() {
    var parsedHint =
        SqrlHint.parse(
                new ParsedObject<>(
                    "index(BTREE, col_a DESC, col_b asc, col_c)", FileLocation.START))
            .get(0);

    var hint = (IndexHint) factory.create(parsedHint);

    assertThat(hint.getIndexType()).isEqualTo(IndexType.BTREE);
    assertThat(hint.getColumnNames()).containsExactly("col_a", "col_b", "col_c");
    assertThat(hint.getDirections())
        .containsExactly(Direction.DESCENDING, Direction.ASCENDING, Direction.ASCENDING);
  }

  @Test
  void givenInvalidIndexColumnDirection_whenCreate_thenThrows() {
    var hint =
        new ParsedObject<>(
            new SqrlHint("index", List.of("BTREE", "col_a sideways")), FileLocation.START);

    assertThatThrownBy(() -> factory.create(hint))
        .isInstanceOf(StatementParserException.class)
        .hasMessageContaining("Expected a column name optionally followed by ASC or DESC");
  }
}
