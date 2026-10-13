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
package com.datasqrl.compile.check;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.compile.check.PassthroughReferences.Found;
import com.datasqrl.compile.check.PassthroughReferences.Table;
import com.datasqrl.compile.check.SymbolIndex.Column;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class PassthroughReferencesTest {

  private static final Table REPORTING =
      table("Reporting", column("employeeid"), column("managerid"), column("level"));
  private static final Table EMPLOYEES = table("Employees", column("employeeid"), column("name"));

  /** A recursive query of the usecases, in the dialect of PostgreSQL */
  private static final String HIERARCHY =
      """
        WITH RECURSIVE employee_hierarchy AS (
          -- Base case: direct reports
          SELECT r.employeeid, r.managerid, 1 as level
          FROM "Reporting" r
          WHERE r.managerid = this.employeeid
          UNION ALL
          SELECT r.employeeid, r.managerid, eh.level + 1 as level
          FROM "Reporting" r
          INNER JOIN employee_hierarchy eh ON r.managerid = eh.employeeid
        )
        SELECT e.employeeid, e.name, eh.level
        FROM employee_hierarchy eh
        JOIN "Employees" e ON eh.employeeid = e.employeeid
        ORDER BY eh.level, e.employeeid;
      """;

  @Test
  void givenRecursiveQuery_whenResolve_thenTablesAndTheirColumnsButNotTheCommonTable() {
    var found =
        PassthroughReferences.resolve(
            HIERARCHY, List.of(REPORTING, EMPLOYEES), Optional.of(EMPLOYEES));

    assertThat(found)
        .extracting(f -> HIERARCHY.substring(f.start(), f.end()), Found::column)
        .containsExactly(
            tuple("\"Reporting\"", List.of()),
            tuple("\"Reporting\"", List.of()),
            tuple("\"Employees\"", List.of()),
            tuple("r.employeeid", List.of("employeeid")),
            tuple("r.managerid", List.of("managerid")),
            tuple("r.managerid", List.of("managerid")),
            tuple("this.employeeid", List.of("employeeid")),
            tuple("r.employeeid", List.of("employeeid")),
            tuple("r.managerid", List.of("managerid")),
            tuple("r.managerid", List.of("managerid")),
            tuple("e.employeeid", List.of("employeeid")),
            tuple("e.name", List.of("name")),
            tuple("e.employeeid", List.of("employeeid")),
            tuple("e.employeeid", List.of("employeeid")));
    assertThat(found)
        .filteredOn(f -> HIERARCHY.substring(f.start(), f.end()).equals("this.employeeid"))
        .extracting(f -> f.table().getObjectName(), f -> f.type().orElseThrow())
        .containsExactly(tuple("Employees", "BIGINT NOT NULL"));
  }

  @Test
  void givenUnqualifiedNames_whenResolve_thenOnlyColumnsOfTheTablesOfTheirQuery() {
    var sql =
        "SELECT name, count(employeeid) AS level, 'name' FROM \"Employees\""
            + " WHERE employeeid = :id AND name::text <> '' GROUP BY name ORDER BY level";

    var found = PassthroughReferences.resolve(sql, List.of(REPORTING, EMPLOYEES), Optional.empty());

    // Not the alias `level`, the string, the parameter or the type of the cast
    assertThat(found)
        .extracting(f -> sql.substring(f.start(), f.end()), f -> f.table().getObjectName())
        .containsExactly(
            tuple("\"Employees\"", "Employees"),
            tuple("name", "Employees"),
            tuple("employeeid", "Employees"),
            tuple("employeeid", "Employees"),
            tuple("name", "Employees"),
            tuple("name", "Employees"));
  }

  @Test
  void givenColumnOfTwoTables_whenResolve_thenLeavesOutTheAmbiguousName() {
    var sql = "SELECT employeeid, r.level FROM \"Reporting\" r, \"Employees\" e";

    var found = PassthroughReferences.resolve(sql, List.of(REPORTING, EMPLOYEES), Optional.empty());

    assertThat(found)
        .extracting(f -> sql.substring(f.start(), f.end()))
        .containsExactly("\"Reporting\"", "\"Employees\"", "r.level");
  }

  private static Table table(String name, Column... columns) {
    return new Table(ObjectIdentifier.of("c", "d", name), List.of(columns));
  }

  private static Column column(String name) {
    return new Column(name, "BIGINT NOT NULL", Optional.empty());
  }
}
