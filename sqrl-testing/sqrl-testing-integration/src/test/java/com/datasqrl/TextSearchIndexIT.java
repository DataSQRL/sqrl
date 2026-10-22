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
package com.datasqrl;

import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.engines.TestContainersForTestGoal;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Plans the generated text search queries on Postgres and checks that each one uses the GIN index
 * the compiler creates for it. Postgres matches an expression index only against an identical
 * expression, so the query and the index must build the same {@code to_tsvector} call.
 */
@Testcontainers
class TextSearchIndexIT {

  private static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @Container
  private static final PostgreSQLContainer postgres =
      new PostgreSQLContainer(TestContainersForTestGoal.POSTGRES_IMAGE);

  @RegisterExtension final CliCompileTestExtension compileExtension = new CliCompileTestExtension();

  @Test
  void givenTextSearchFunctions_whenQueriesPlannedOnPostgres_thenEachUsesGinIndex()
      throws Exception {
    var packageFile = USECASE_DIR.resolve("pg-text-search-compile").resolve("package.json");
    var hook =
        compileExtension.execute(
            packageFile.getParent(), "compile", packageFile.getFileName().toString());
    assertThat(hook.isFailed()).as(hook.getMessages()).isFalse();

    var planDir = compileExtension.getPlanDir();
    var searchQueries =
        SqrlObjectMapper.INSTANCE
            .readTree(planDir.resolve("vertx.json").toFile())
            .findValues("sql")
            .stream()
            .map(JsonNode::asText)
            .filter(sql -> sql.contains("@@"))
            .distinct()
            .toList();
    assertThat(searchQueries).hasSize(2);

    var props = new Properties();
    props.setProperty("user", postgres.getUsername());
    props.setProperty("password", postgres.getPassword());
    // The simple query protocol lets EXPLAIN (GENERIC_PLAN) plan queries with unbound $n parameters
    props.setProperty("preferQueryMode", "simple");
    try (var conn = DriverManager.getConnection(postgres.getJdbcUrl(), props);
        var stmt = conn.createStatement()) {
      stmt.execute(Files.readString(planDir.resolve("postgres-schema.sql")));
      // The queries only filter and sort on the text search, so with sequential scans disabled
      // Postgres still plans one only when the GIN index does not match the query
      stmt.execute("SET enable_seqscan = off");

      for (var sql : searchQueries) {
        assertThat(explain(stmt, sql))
            .as("Plan of %s", sql)
            .contains("Bitmap Index Scan on")
            .doesNotContain("Seq Scan");
      }
    }
  }

  private static String explain(Statement stmt, String sql) throws SQLException {
    var plan = new StringBuilder();
    try (var rs = stmt.executeQuery("EXPLAIN (GENERIC_PLAN) " + sql)) {
      while (rs.next()) {
        plan.append(rs.getString(1)).append('\n');
      }
    }
    return plan.toString();
  }
}
