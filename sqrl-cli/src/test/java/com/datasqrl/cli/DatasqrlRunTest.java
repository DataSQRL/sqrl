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
package com.datasqrl.cli;

import static com.datasqrl.env.EnvVariableNames.POSTGRES_JDBC_URL;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PASSWORD;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class DatasqrlRunTest {

  @TempDir private Path tempDir;

  private Configuration flinkConfig;
  private Map<String, String> env;

  private DatasqrlRun underTest;

  @BeforeEach
  void setup() {
    flinkConfig = mock(Configuration.class);
    env = new HashMap<>();

    underTest = DatasqrlRun.nonBlocking(tempDir.resolve("plan"), null, flinkConfig, env);
  }

  @Test
  void run_whenCompiledPlanReferencesMissingEnvVar_propagatesClearError() throws Exception {
    var planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);
    Files.writeString(
        planDir.resolve("flink-sql.sql"),
        "CREATE TABLE t (id INT) WITH ('connector' = 'datagen', 'id' = '${NON_EXISTING_ENV_VAR}');\n");

    var realFlinkConfig = new Configuration();
    realFlinkConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

    var sqrlConfig = mock(PackageJson.class, RETURNS_DEEP_STUBS);
    when(sqrlConfig.getCompilerConfig().compileFlinkPlan()).thenReturn(false);

    var run = DatasqrlRun.nonBlocking(planDir, sqrlConfig, realFlinkConfig, new HashMap<>());

    assertThatThrownBy(run::run)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("NON_EXISTING_ENV_VAR");
  }

  @Test
  void returnsEmptyWhenNoSavepointDirConfigured() {
    assertThat(underTest.getLastSavepoint()).isEmpty();
  }

  @Test
  void usesSavepointDirFromFlinkConfig() throws Exception {
    // create two savepoint directories with different creation times
    Files.createDirectory(tempDir.resolve("sp1"));
    Thread.sleep(1000);
    Path sp2 = Files.createDirectory(tempDir.resolve("sp2"));

    String uri = tempDir.toUri().toString();
    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY)).thenReturn(uri);

    var result = underTest.getLastSavepoint();

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(sp2.toAbsolutePath().toString());
  }

  @Test
  void returnsEmptyIfSavepointDirConfigBlank() {
    underTest = DatasqrlRun.nonBlocking(tempDir.resolve("plan"), null, flinkConfig, env);

    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY)).thenReturn(" ");

    assertThat(underTest.getLastSavepoint()).isEmpty();
  }

  @Test
  void returnsEmptyIfDirectoryDoesNotExist() {
    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
        .thenReturn("file:///nonexistent-dir");

    assertThat(underTest.getLastSavepoint()).isEmpty();
  }

  @Test
  void givenPlanWithStandaloneExtensionStatements_whenRun_thenExecutesThemAfterRegularStatements()
      throws Exception {
    var run = givenRunStoppingAfterPostgresInit();
    var connection =
        givenPostgresPlan(
            """
        {
          "statements": [
            {"name": "my_table", "type": "TABLE", "sql": "CREATE TABLE my_table (id INT)"}
          ],
          "standaloneExtensionStatements": [
            {"name": "partman", "type": "EXTENSION", "sql": "CREATE EXTENSION pg_partman"},
            {"name": "partman_parent", "type": "EXTENSION", "sql": "SELECT create_parent()"}
          ]
        }
        """);
    var statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);

    try (MockedStatic<DriverManager> driverManagerMocked = mockStatic(DriverManager.class)) {
      driverManagerMocked
          .when(() -> DriverManager.getConnection("jdbc:postgresql://localhost/db", "user", "pw"))
          .thenReturn(connection);

      assertThatThrownBy(run::run).hasMessageContaining("NON_EXISTING_ENV_VAR");
    }

    var order = inOrder(statement);
    order.verify(statement).execute("CREATE TABLE my_table (id INT)");
    order.verify(statement).execute("CREATE EXTENSION pg_partman");
    order.verify(statement).execute("SELECT create_parent()");
  }

  @Test
  void givenFailingStandaloneExtensionStatement_whenRun_thenContinuesNonFatally() throws Exception {
    var run = givenRunStoppingAfterPostgresInit();
    var connection =
        givenPostgresPlan(
            """
        {
          "statements": [
            {"name": "my_table", "type": "TABLE", "sql": "CREATE TABLE my_table (id INT)"}
          ],
          "standaloneExtensionStatements": [
            {"name": "partman", "type": "EXTENSION", "sql": "CREATE EXTENSION pg_partman"},
            {"name": "partman_parent", "type": "EXTENSION", "sql": "SELECT create_parent()"}
          ]
        }
        """);
    var statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.execute(anyString()))
        .thenReturn(true)
        .thenThrow(new SQLException("extension \"pg_partman\" is not available"))
        .thenReturn(true);

    try (MockedStatic<DriverManager> driverManagerMocked = mockStatic(DriverManager.class)) {
      driverManagerMocked
          .when(() -> DriverManager.getConnection("jdbc:postgresql://localhost/db", "user", "pw"))
          .thenReturn(connection);

      // reaching the Flink sentinel error proves the extension failure did not abort the run
      assertThatThrownBy(run::run).hasMessageContaining("NON_EXISTING_ENV_VAR");
    }

    var order = inOrder(statement);
    order.verify(statement).execute("CREATE EXTENSION pg_partman");
    order.verify(statement).execute("SELECT create_parent()");
  }

  @Test
  void givenPlanWithoutPostgresJson_whenRun_thenSkipsDatabaseConnection() throws Exception {
    var run = givenRunStoppingAfterPostgresInit();

    try (MockedStatic<DriverManager> driverManagerMocked = mockStatic(DriverManager.class)) {
      assertThatThrownBy(run::run).hasMessageContaining("NON_EXISTING_ENV_VAR");

      driverManagerMocked.verifyNoInteractions();
    }
  }

  /**
   * The returned instance runs Postgres init for real, then fails at the Flink stage on a missing
   * env var, so tests can drive {@code initPostgres()} through the public {@code run()} method
   * alone.
   */
  private DatasqrlRun givenRunStoppingAfterPostgresInit() throws Exception {
    var planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);
    Files.writeString(
        planDir.resolve("flink-sql.sql"),
        "CREATE TABLE t (id INT) WITH ('connector' = 'datagen', 'id' = '${NON_EXISTING_ENV_VAR}');\n");

    var realFlinkConfig = new Configuration();
    realFlinkConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

    var sqrlConfig = mock(PackageJson.class, RETURNS_DEEP_STUBS);
    when(sqrlConfig.getCompilerConfig().compileFlinkPlan()).thenReturn(false);

    return DatasqrlRun.nonBlocking(planDir, sqrlConfig, realFlinkConfig, env);
  }

  private Connection givenPostgresPlan(String postgresJson) throws Exception {
    var planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);
    Files.writeString(planDir.resolve("postgres.json"), postgresJson);

    env.put(POSTGRES_JDBC_URL, "jdbc:postgresql://localhost/db");
    env.put(POSTGRES_USERNAME, "user");
    env.put(POSTGRES_PASSWORD, "pw");

    return mock(Connection.class);
  }

  @Test
  void fallsBackToPathStringIfUriSyntaxInvalid() throws Exception {
    // Create one savepoint
    Files.createDirectory(tempDir.resolve("sp1"));

    // Provide an invalid URI (e.g., no scheme)
    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
        .thenReturn(tempDir.toAbsolutePath().toString());

    var result = underTest.getLastSavepoint();

    assertThat(result).isPresent();
    assertThat(result.get()).endsWith("sp1");
  }
}
