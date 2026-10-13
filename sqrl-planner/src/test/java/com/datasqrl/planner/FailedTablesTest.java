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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.planner.parser.SQLStatement;
import org.junit.jupiter.api.Test;

class FailedTablesTest {

  /** A statement of Flink SQL, which the tables are recorded from by their source */
  private static final SQLStatement FLINK_STATEMENT = new SQLStatement() {};

  private final FailedTables failedTables = new FailedTables();

  @Test
  void givenFailedTable_whenStatementUsesIt_thenItIsSkippedButNotForAKeywordOrString() {
    failedTables.recordFailure(
        FLINK_STATEMENT, "CREATE TABLE `Order` (id INT) WITH ('connector' = 'x');", false, null);

    assertThat(failedTables.isReferencedBy("Totals := SELECT * FROM `Order`;")).isTrue();
    assertThat(failedTables.isReferencedBy("Totals := SELECT * FROM Orders ORDER BY id;"))
        .isFalse();
    assertThat(failedTables.isReferencedBy("Totals := SELECT 'Order' AS name FROM Orders;"))
        .isFalse();
  }

  @Test
  void givenFailedTableOfImportedScript_whenStatementUsesItsNamespace_thenItIsSkipped() {
    failedTables.recordFailure(
        FLINK_STATEMENT, "CREATE TABLE Orders (id INT) WITH ('connector' = 'x');", false, "src");

    assertThat(failedTables.isReferencedBy("Totals := SELECT * FROM src.Orders;")).isTrue();
    assertThat(failedTables.isReferencedBy("Totals := SELECT * FROM Orders;")).isFalse();
  }

  @Test
  void givenSkippedUseStatement_whenMorePlanned_thenTheContextFailed() {
    failedTables.recordFailure(
        FLINK_STATEMENT, "CREATE CATALOG iceberg WITH ('type' = 'iceberg');", false, null);
    assertThat(failedTables.isReferencedBy("USE CATALOG iceberg;")).isTrue();
    assertThat(failedTables.isContextFailed()).isFalse();

    failedTables.recordSkipped(FLINK_STATEMENT, "USE CATALOG iceberg;", null);

    // The statements after it would run in another catalog than intended
    assertThat(failedTables.isContextFailed()).isTrue();
  }

  @Test
  void givenFailedUseOfDatabase_whenContextIsRestored_thenItIsNotFailed() {
    failedTables.recordFailure(FLINK_STATEMENT, "/* the database */ USE my_database;", false, null);
    assertThat(failedTables.isContextFailed()).isTrue();

    // E.g. after an imported script, which has a context of its own
    failedTables.restoreContextFailed(false);

    assertThat(failedTables.isContextFailed()).isFalse();
  }
}
