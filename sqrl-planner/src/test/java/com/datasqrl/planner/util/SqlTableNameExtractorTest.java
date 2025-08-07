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
package com.datasqrl.planner.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class SqlTableNameExtractorTest {

  @Test
  void givenSimpleQuery_whenFindTableNames_thenReturnsTableName() {
    String sql = "SELECT * FROM orders";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactly("orders");
  }

  @Test
  void givenQueryWithJoin_whenFindTableNames_thenReturnsBothTables() {
    String sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("orders", "customers");
  }

  @Test
  void givenQueryWithCTE_whenFindTableNames_thenExcludesCTEFromResults() {
    String sql =
        """
      WITH recent_orders AS (
          SELECT * FROM orders WHERE order_date >= CURRENT_DATE - 1
      ),
      enriched AS (
          SELECT r.*, c.name FROM recent_orders r
          JOIN customers c ON c.id = r.customer_id
      )
      SELECT * FROM enriched JOIN audit_log al ON al.order_id = enriched.id
      """;

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("orders", "customers", "audit_log");
  }

  @Test
  void givenQueryWithQuotedIdentifiers_whenFindTableNames_thenHandlesAllQuoteTypes() {
    String sql = "SELECT * FROM `Orders` JOIN \"Customers\" c ON c.id = [Orders].customer_id";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("Orders", "Customers");
  }

  @Test
  void givenQueryWithComments_whenFindTableNames_thenIgnoresComments() {
    String sql =
        """
      -- This is a comment with table_name_in_comment
      SELECT * FROM orders /* another comment with products */
      JOIN customers c ON c.id = o.customer_id
      """;

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("orders", "customers");
  }

  @Test
  void givenRecursiveCTEQuery_whenFindTableNames_thenExtractsTablesExcludingCTE() {
    String sql =
        """
      WITH RECURSIVE employee_hierarchy AS (
        -- Base case: direct reports
        SELECT r.employeeid, r.managerid, 1 as level
        FROM Reporting r
        WHERE r.managerid = this.employeeid

        UNION ALL

        -- Recursive case: reports of reports
        SELECT r.employeeid, r.managerid, eh.level + 1 as level
        FROM Reporting r
        INNER JOIN employee_hierarchy eh ON r.managerid = eh.employeeid
      )
      SELECT e.employeeid, e.name, eh.level
      FROM employee_hierarchy eh
      JOIN Employees e ON eh.employeeid = e.employeeid
      ORDER BY eh.level, e.employeeid
      """;

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("Reporting", "Employees");
  }

  @Test
  void givenQueryWithStringLiterals_whenFindTableNames_thenIgnoresLiterals() {
    String sql = "SELECT * FROM orders WHERE description = 'FROM customers table'";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactly("orders");
  }

  @Test
  void givenQueryWithSubquery_whenFindTableNames_thenHandlesSubqueries() {
    String sql = "SELECT * FROM (SELECT * FROM orders) o JOIN customers c ON o.customer_id = c.id";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("orders", "customers");
  }

  @Test
  void givenQueryWithMultipleJoinTypes_whenFindTableNames_thenHandlesAllJoinTypes() {
    String sql =
        """
      SELECT * FROM orders o
      LEFT JOIN customers c ON o.customer_id = c.id
      RIGHT JOIN products p ON o.product_id = p.id
      INNER JOIN categories cat ON p.category_id = cat.id
      """;

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("orders", "customers", "products", "categories");
  }

  @Test
  void givenQueryWithDollarQuotedLiterals_whenFindTableNames_thenIgnoresDollarQuotes() {
    String sql = "SELECT * FROM orders WHERE description = $$FROM customers table$$";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactly("orders");
  }

  @Test
  void givenComplexNestedCTE_whenFindTableNames_thenHandlesNestedStructures() {
    String sql =
        """
      WITH recursive_cte AS (
        SELECT * FROM base_table
        UNION ALL
        SELECT * FROM recursive_cte r JOIN child_table c ON r.id = c.parent_id
      ),
      final_cte AS (
        SELECT * FROM recursive_cte JOIN lookup_table l ON recursive_cte.type = l.type
      )
      SELECT * FROM final_cte JOIN results r ON final_cte.id = r.id
      """;

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result)
        .containsExactlyInAnyOrder("base_table", "child_table", "lookup_table", "results");
  }

  @Test
  void givenEmptyString_whenFindTableNames_thenReturnsEmptySet() {
    String sql = "";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).isEmpty();
  }

  @Test
  void
      givenQueryWithIdentifiersContainingDollarsAndUnderscores_whenFindTableNames_thenHandlesSpecialCharacters() {
    String sql =
        "SELECT * FROM order_items_$temp JOIN customer_data_2024 c ON c.id = order_items_$temp.customer_id";

    Set<String> result = SqlTableNameExtractor.findTableNames(sql);

    assertThat(result).containsExactlyInAnyOrder("order_items_$temp", "customer_data_2024");
  }
}
