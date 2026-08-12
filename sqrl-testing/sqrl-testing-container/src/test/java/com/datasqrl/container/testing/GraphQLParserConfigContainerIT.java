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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.env.EnvVariableNames;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class GraphQLParserConfigContainerIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String QUERY =
      "{\"query\":\"query ParserLimits { CustomersByName(inputName: \\\"Bob Jones\\\") { customerid name } }\"}";

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("server-functions");

  @RegisterExtension
  static PostgresContainerExtension postgres =
      new PostgresContainerExtension(sqrl, GraphQLParserConfigContainerIT::executeStatements);

  private static void executeStatements(Statement stmt) throws SQLException {
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS \"Customers\" ("
            + "\"customerid\" BIGINT NOT NULL,"
            + "\"email\" TEXT NOT NULL,"
            + "\"name\" TEXT NOT NULL,"
            + "\"lastUpdated\" BIGINT NOT NULL,"
            + "\"timestamp\" TIMESTAMP WITH TIME ZONE NOT NULL,"
            + "PRIMARY KEY (\"customerid\",\"lastUpdated\"))");

    stmt.execute(
        "INSERT INTO \"Customers\" VALUES (1, 'bob.jones@example.com', 'Bob Jones', 1730700002000, '2025-11-11T00:00:00Z')");
  }

  @Test
  @SneakyThrows
  void givenLoweredMaxTokens_whenGraphQLQueryExceedsIt_thenParsingIsCancelled() {
    postgres.startPostgreSQLContainer();
    sqrl.compileSqrlProject();
    applyMaxTokens(5);

    startServer();

    try (var response = sqrl.executeGraphQLQuery(QUERY)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      var responseJson = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
      assertThat(responseJson.at("/errors/0/message").asText())
          .contains("More than 5 'grammar' tokens have been presented");
    }
  }

  @Test
  @SneakyThrows
  void givenDefaultParserConfig_whenGraphQLQueryRuns_thenItIsParsedSuccessfully() {
    postgres.startPostgreSQLContainer();
    sqrl.compileSqrlProject();

    startServer();

    try (var response = sqrl.executeGraphQLQuery(QUERY)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      var responseJson = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
      assertThat(responseJson.has("errors")).isFalse();
      assertThat(responseJson.at("/data/CustomersByName/0/name").asText()).isEqualTo("Bob Jones");
    }
  }

  private void startServer() {
    sqrl.startGraphQLServer(
        container ->
            container
                .withEnv(EnvVariableNames.POSTGRES_HOST, "postgresql")
                .withEnv(EnvVariableNames.POSTGRES_USERNAME, postgres.getPostgresql().getUsername())
                .withEnv(EnvVariableNames.POSTGRES_PASSWORD, postgres.getPostgresql().getPassword())
                .withEnv(
                    EnvVariableNames.POSTGRES_DATABASE, postgres.getPostgresql().getDatabaseName())
                .withEnv(EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092"));
  }

  @SneakyThrows
  private void applyMaxTokens(int maxTokens) {
    var configPath = sqrl.getTestDir().resolve("build/deploy/plan/vertx-config.json");
    var config = (ObjectNode) MAPPER.readTree(configPath.toFile());
    config.set("graphQLParserConfig", MAPPER.createObjectNode().put("maxTokens", maxTokens));
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(configPath.toFile(), config);
  }
}
