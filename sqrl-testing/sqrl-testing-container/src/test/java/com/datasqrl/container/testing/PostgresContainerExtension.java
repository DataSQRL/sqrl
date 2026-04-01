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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class PostgresContainerExtension implements AfterEachCallback {

  private final SqrlContainerExtension sqrl;
  private final StatementConsumer schemaInitializer;

  @Getter private PostgreSQLContainer postgresql;

  @FunctionalInterface
  public interface StatementConsumer {
    void accept(Statement stmt) throws SQLException;
  }

  public PostgresContainerExtension(
      SqrlContainerExtension sqrl, StatementConsumer schemaInitializer) {
    this.sqrl = Objects.requireNonNull(sqrl);
    this.schemaInitializer = Objects.requireNonNull(schemaInitializer);
  }

  @SneakyThrows
  public void startPostgreSQLContainer() {
    if (postgresql == null) {
      var network = sqrl.getNetwork();
      if (network == null) {
        throw new IllegalStateException(
            "SqrlContainerExtension must be declared before PostgresContainerExtension "
                + "so that the shared Docker network is initialized before Postgres starts.");
      }
      postgresql =
          new PostgreSQLContainer(DockerImageName.parse("postgres:17"))
              .withDatabaseName("datasqrl")
              .withUsername("datasqrl")
              .withPassword("password")
              .withNetwork(network)
              .withNetworkAliases("postgresql");
      postgresql.start();
      log.info("PostgreSQL container started on port {}", postgresql.getMappedPort(5432));

      try (var connection = postgresql.createConnection("")) {
        try (var stmt = connection.createStatement()) {
          schemaInitializer.accept(stmt);
        }
      }
      log.info("Schema initialized");
    }
  }

  public void compileAndStartServerWithDatabase() {
    compileAndStartServerWithDatabase(null);
  }

  public void compileAndStartServerWithDatabase(@Nullable String packageFile) {
    startPostgreSQLContainer();
    sqrl.compileSqrlProject(packageFile);

    sqrl.startGraphQLServer(
        container ->
            container
                .withEnv(POSTGRES_HOST, "postgresql")
                .withEnv(POSTGRES_USERNAME, postgresql.getUsername())
                .withEnv(POSTGRES_PASSWORD, postgresql.getPassword())
                .withEnv(POSTGRES_DATABASE, postgresql.getDatabaseName())
                .withEnv(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092"));
  }

  @Override
  public void afterEach(ExtensionContext context) {
    if (postgresql != null) {
      postgresql.stop();
      postgresql = null;
    }
  }
}
