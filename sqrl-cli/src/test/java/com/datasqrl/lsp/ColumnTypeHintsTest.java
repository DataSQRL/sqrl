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
package com.datasqrl.lsp;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.lsp.ColumnTypeHints.SelectItem;
import org.junit.jupiter.api.Test;

class ColumnTypeHintsTest {

  @Test
  void givenQuery_whenSelectItems_thenSplitsTheTopLevelSelectList() {
    var query =
        "SELECT DISTINCT a, COALESCE(b, 0) AS b, 'x,y' AS c -- d, e\n FROM T WHERE x IN (1, 2)";

    var items = ColumnTypeHints.selectItems(query);

    assertThat(items)
        .extracting(item -> item.text().strip())
        .containsExactly("a", "COALESCE(b, 0) AS b", "AS c");
    assertThat(items.get(1).start()).isEqualTo(query.indexOf(",") + 1);
    assertThat(items.get(2).text()).doesNotContain("x,y");
  }

  @Test
  void givenNestedQueries_whenSelectItems_thenUsesTheOuterSelectList() {
    var query = "WITH t AS (SELECT a AS inner_a FROM T) SELECT (SELECT 1) AS one, t.a FROM t;";

    assertThat(ColumnTypeHints.selectItems(query))
        .extracting(SelectItem::text)
        .extracting(String::strip)
        .containsExactly("(SELECT 1) AS one", "t.a");
  }

  @Test
  void givenQueryWithoutFrom_whenSelectItems_thenEndsAtTheStatementEnd() {
    assertThat(ColumnTypeHints.selectItems("SELECT 1 AS one, 2 AS two;"))
        .extracting(item -> item.text().strip())
        .containsExactly("1 AS one", "2 AS two");
  }
}
