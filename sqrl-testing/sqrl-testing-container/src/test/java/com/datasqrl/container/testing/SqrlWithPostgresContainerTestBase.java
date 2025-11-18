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
package com.datasqrl.container.testing;

import static com.datasqrl.env.EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_DATABASE;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_HOST;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PASSWORD;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_USERNAME;

import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Abstract base class for container tests that require a PostgreSQL database. Extends {@link
 * SqrlContainerTestBase} and adds PostgreSQL container management.
 */
@Slf4j
public abstract class SqrlWithPostgresContainerTestBase extends SqrlContainerTestBase {

  protected PostgreSQLContainer<?> postgresql;

  /**
   * Abstract method that subclasses must implement to create and populate their specific test
   * tables in the PostgreSQL database.
   */
  protected abstract void executeStatements(Statement stmt) throws SQLException;

  /**
   * Starts the PostgreSQL container if not already running.
   *
   * <p>After starting the container, calls {@link #executeStatements(Statement)} to initialize the
   * database schema.
   */
  @SneakyThrows
  protected void startPostgreSQLContainer() {
    if (postgresql == null) {
      postgresql =
          new PostgreSQLContainer<>("postgres:17")
              .withDatabaseName("datasqrl")
              .withUsername("datasqrl")
              .withPassword("password")
              .withNetwork(sharedNetwork)
              .withNetworkAliases("postgresql");
      postgresql.start();
      log.info("PostgreSQL container started on port {}", postgresql.getMappedPort(5432));

      try (var connection = postgresql.createConnection("")) {
        try (var stmt = connection.createStatement()) {
          executeStatements(stmt);
        }
      }
    }
  }

  protected void compileAndStartServerWithDatabase(Path workingDir) {
    compileAndStartServerWithDatabase(workingDir, null);
  }

  protected void compileAndStartServerWithDatabase(Path workingDir, @Nullable String packageFile) {
    startPostgreSQLContainer();
    compileSqrlProject(workingDir, packageFile);

    startGraphQLServer(
        workingDir,
        container ->
            container
                .withEnv(POSTGRES_HOST, "postgresql")
                .withEnv(POSTGRES_USERNAME, postgresql.getUsername())
                .withEnv(POSTGRES_PASSWORD, postgresql.getPassword())
                .withEnv(POSTGRES_DATABASE, postgresql.getDatabaseName())
                .withEnv(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092"));
  }

  @Override
  protected void commonTearDown() {
    super.commonTearDown();
    if (postgresql != null) {
      postgresql.stop();
      postgresql = null;
    }
  }
}
