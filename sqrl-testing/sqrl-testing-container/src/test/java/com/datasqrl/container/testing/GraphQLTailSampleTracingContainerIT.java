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
import static org.awaitility.Awaitility.await;

import com.datasqrl.env.EnvVariableNames;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class GraphQLTailSampleTracingContainerIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("server-functions");

  @RegisterExtension
  static PostgresContainerExtension postgres =
      new PostgresContainerExtension(sqrl, GraphQLTailSampleTracingContainerIT::executeStatements);

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
  void givenTailSampleTracingEnabled_whenGraphQLQueryHasMultipleFields_thenWarnTraceIsLogged() {
    postgres.startPostgreSQLContainer();
    sqrl.compileSqrlProject();
    applyTestTailSampleTracingConfig();

    sqrl.startGraphQLServer(
        container ->
            container
                .withEnv(EnvVariableNames.POSTGRES_HOST, "postgresql")
                .withEnv(EnvVariableNames.POSTGRES_USERNAME, postgres.getPostgresql().getUsername())
                .withEnv(EnvVariableNames.POSTGRES_PASSWORD, postgres.getPostgresql().getPassword())
                .withEnv(
                    EnvVariableNames.POSTGRES_DATABASE, postgres.getPostgresql().getDatabaseName())
                .withEnv(EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092"));

    try (var response =
        sqrl.executeGraphQLQuery(
            "{\"query\":\"query TailTrace { CustomersByName(inputName: \\\"Bobblabla\\\") { customerid name } CustomersById(a: 15, b: 10) { customerid name } }\"}")) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      var responseJson = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
      assertThat(responseJson.has("errors")).isFalse();
      assertThat(responseJson.at("/data/CustomersByName/0/name").asText()).isEqualTo("Bob Jones");
      assertThat(responseJson.at("/data/CustomersById/0/name").asText()).isEqualTo("Bob Jones");
    }

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              var event = getTailTraceLogEvent();
              assertThat(event.get("event").asText()).isEqualTo("sqrl.graphql.query.tail_trace");
              assertThat(event.get("operationName").asText()).isEqualTo("TailTrace");
              assertThat(event.get("sampleReason").asText()).isEqualTo("slow");
              assertThat(event.get("thresholdMs").asLong()).isEqualTo(1);
              assertThat(event.get("fields")).hasSizeGreaterThanOrEqualTo(2);
              assertThat(event.get("fields").findValuesAsText("path"))
                  .contains("/CustomersByName", "/CustomersById");
            });
  }

  @Test
  @SneakyThrows
  void givenDefaultThresholdAndLowQueryThreshold_whenGraphQLQueryRuns_thenWarnTraceIsLogged() {
    postgres.startPostgreSQLContainer();
    sqrl.compileSqrlProject();
    applyQuerySpecificTailSampleTracingConfig();

    sqrl.startGraphQLServer(
        container ->
            container
                .withEnv(EnvVariableNames.POSTGRES_HOST, "postgresql")
                .withEnv(EnvVariableNames.POSTGRES_USERNAME, postgres.getPostgresql().getUsername())
                .withEnv(EnvVariableNames.POSTGRES_PASSWORD, postgres.getPostgresql().getPassword())
                .withEnv(
                    EnvVariableNames.POSTGRES_DATABASE, postgres.getPostgresql().getDatabaseName())
                .withEnv(EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092"));

    try (var response =
        sqrl.executeGraphQLQuery(
            "{\"query\":\"query QuerySpecificTailTrace { CustomersByName(inputName: \\\"Bobblabla\\\") { customerid name } CustomersById(a: 15, b: 10) { customerid name } }\"}")) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      var responseJson = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
      assertThat(responseJson.has("errors")).isFalse();
      assertThat(responseJson.at("/data/CustomersByName/0/name").asText()).isEqualTo("Bob Jones");
      assertThat(responseJson.at("/data/CustomersById/0/name").asText()).isEqualTo("Bob Jones");
    }

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              var event = getTailTraceLogEvent();
              assertThat(event.get("operationName").asText()).isEqualTo("QuerySpecificTailTrace");
              assertThat(event.get("sampleReason").asText()).isEqualTo("slow");
              assertThat(event.get("thresholdMs").asLong()).isEqualTo(1);
              assertThat(event.get("fields")).hasSizeGreaterThanOrEqualTo(2);
              assertThat(event.get("fields").findValuesAsText("path"))
                  .contains("/CustomersByName", "/CustomersById");
              assertThat(event.get("fields"))
                  .anySatisfy(
                      field -> {
                        assertThat(field.get("path").asText()).isEqualTo("/CustomersByName");
                        assertThat(field.get("thresholdMs").asLong()).isEqualTo(1);
                      });
            });
  }

  @SneakyThrows
  private void applyTestTailSampleTracingConfig() {
    var configPath = sqrl.getTestDir().resolve("build/deploy/plan/vertx-config.json");
    var config = (ObjectNode) MAPPER.readTree(configPath.toFile());
    var tracingConfig = MAPPER.createObjectNode();
    tracingConfig.put("enabled", true);
    tracingConfig.put("defaultThresholdMs", 1);
    tracingConfig.put("maxLoggedTracesPerMinute", 0);
    tracingConfig.put("maxFieldTraces", 10);
    config.set("graphQLTailSampleTracingConfig", tracingConfig);
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(configPath.toFile(), config);
  }

  @SneakyThrows
  private void applyQuerySpecificTailSampleTracingConfig() {
    var configPath = sqrl.getTestDir().resolve("build/deploy/plan/vertx-config.json");
    var config = (ObjectNode) MAPPER.readTree(configPath.toFile());
    var tracingConfig = MAPPER.createObjectNode();
    var thresholds = MAPPER.createObjectNode();

    thresholds.put("Query.CustomersByName", 1);
    tracingConfig.put("enabled", true);
    tracingConfig.put("maxLoggedTracesPerMinute", 0);
    tracingConfig.put("maxFieldTraces", 10);
    tracingConfig.set("thresholds", thresholds);
    config.set("graphQLTailSampleTracingConfig", tracingConfig);
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(configPath.toFile(), config);
  }

  @SneakyThrows
  private JsonNode getTailTraceLogEvent() {
    var marker = "{\"event\":\"sqrl.graphql.query.tail_trace\"";
    var logs = sqrl.getServerContainer().getLogs();
    var eventStart = logs.indexOf(marker);
    assertThat(eventStart)
        .as("Expected tail trace event in server logs:\n%s", logs)
        .isNotNegative();

    var eventEnd = logs.indexOf(System.lineSeparator(), eventStart);
    var eventJson =
        eventEnd < 0 ? logs.substring(eventStart) : logs.substring(eventStart, eventEnd);
    return MAPPER.readTree(eventJson);
  }
}
