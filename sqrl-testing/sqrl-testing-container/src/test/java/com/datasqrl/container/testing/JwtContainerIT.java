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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.jsonwebtoken.Jwts;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

@Slf4j
public class JwtContainerIT extends SqrlContainerTestBase {

  private PostgreSQLContainer<?> postgresql;

  @Override
  protected String getTestCaseName() {
    return "jwt-authorized";
  }

  private void startPostgreSQLContainer() {
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
      createTestTables();
    }
  }

  @SneakyThrows
  private void createTestTables() {
    // Create and populate tables as defined in jwt-authorized.sqrl
    try (var connection = postgresql.createConnection("")) {
      try (var stmt = connection.createStatement()) {
        // Create MyTable and populate with values 1-10
        stmt.execute("CREATE TABLE IF NOT EXISTS \"MyTable\" (val INTEGER)");
        stmt.execute(
            "INSERT INTO \"MyTable\" (val) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");

        // Create MyStringTable and populate with 'a', 'b', 'c', 'd'
        stmt.execute("CREATE TABLE IF NOT EXISTS \"MyStringTable\" (val VARCHAR(1))");
        stmt.execute("INSERT INTO \"MyStringTable\" (val) VALUES ('a'), ('b'), ('c'), ('d')");

        log.info("Test tables created and populated successfully");
      }
    }
  }

  private void compileAndStartServerWithDatabase(String scriptName, Path testDir) {
    startPostgreSQLContainer();
    compileSqrlScript(scriptName, testDir);
    startGraphQLServer(
        testDir,
        container -> {
          container
              .withEnv(POSTGRES_HOST, "postgresql")
              .withEnv(POSTGRES_USERNAME, postgresql.getUsername())
              .withEnv(POSTGRES_PASSWORD, postgresql.getPassword())
              .withEnv(POSTGRES_DATABASE, postgresql.getDatabaseName())
              .withEnv(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
        });
  }

  @Override
  protected void commonTearDown() {
    super.commonTearDown();
    if (postgresql != null) {
      postgresql.stop();
      postgresql = null;
    }
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenServerStarted_thenUnauthorizedRequestsReturn401() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenServerStartedWithValidJwt_thenAuthorizedRequestsSucceed() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}", generateJwtToken());
    validateBasicGraphQLResponse(response);
  }

  @Test
  @SneakyThrows
  void
      givenJwtEnabledScript_whenServerStartedWithMismatchedAlgorithm_thenReturns401WithDetailedError() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    // Generate token with RS256 algorithm while server expects HS256
    var response =
        executeGraphQLQuery("{\"query\":\"query { __typename }\"}", generateRS256JwtToken());

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

    // Verify the response contains detailed error information in JSON format
    var responseBody = EntityUtils.toString(response.getEntity());
    assertThat(responseBody).contains("\"error\"");
    assertThat(responseBody).contains("\"JWT auth failed\"");
    assertThat(responseBody).contains("\"cause\"");
    assertThat(responseBody).contains("\"NoSuchKeyIdException: algorithm [RS256]: <null>\"");
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenQueryHasCorruptJwt_thenReturns401WithDetailedError() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    // Generate token with RS256 algorithm while server expects HS256
    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}", "dummy-invalid-jwt");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

    // Verify the response contains detailed error information in JSON format
    var responseBody = EntityUtils.toString(response.getEntity());
    System.out.println(responseBody);
    assertThat(responseBody).contains("\"error\"");
    assertThat(responseBody).contains("\"JWT auth failed\"");
    assertThat(responseBody).contains("\"cause\"");
    assertThat(responseBody).contains("\"IllegalArgumentException: Invalid format for JWT\"");
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenMcpClientAccessesWithoutAuth_thenFails() {
    compileAndStartServer("jwt-authorized.sqrl", testDir);

    var mcpUrl =
        String.format("http://localhost:%d/mcp", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    log.info("Testing MCP endpoint without JWT: {}", mcpUrl);

    // Create MCP client without authentication
    var transport = HttpClientStreamableHttpTransport.builder(mcpUrl).build();
    var client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build();

    // Should fail when trying to initialize due to lack of authentication
    assertThatThrownBy(() -> client.initialize())
        .satisfies(
            ex -> {
              var fullMessage = getFullExceptionMessage(ex);
              assertThat(fullMessage).containsIgnoringCase("JWT auth failed");
            });

    client.close();
  }

  @Test
  @SneakyThrows
  void givenJwtEnabledScript_whenMcpClientAccessesWithValidJwt_thenSucceeds() {
    compileAndStartServerWithDatabase("jwt-authorized.sqrl", testDir);

    var mcpUrl =
        String.format("http://localhost:%d/mcp", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    var jwtToken = generateJwtToken();
    log.info("Testing MCP endpoint with valid JWT: {}", mcpUrl);

    // Create MCP client with JWT authentication
    var transport =
        HttpClientStreamableHttpTransport.builder(mcpUrl)
            .customizeRequest(r -> r.header("Authorization", "Bearer " + jwtToken))
            .build();

    try (var client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build(); ) {
      // Should succeed with proper JWT
      client.initialize();

      var tools = client.listTools();

      log.info("Successfully listed {} tools", tools.tools().size());
      assertThat(tools).isNotNull();
      assertThat(tools.tools()).isNotNull();
      assertThat(tools.tools()).isNotEmpty();

      // Test calling a tool that doesn't require auth metadata
      var myTableTool =
          tools.tools().stream()
              .filter(tool -> "GetMyTable".equals(tool.name()))
              .findFirst()
              .orElseThrow(() -> new RuntimeException("GetMyTable tool not found"));
      log.info("Testing tool call: {}", myTableTool.name());

      var callRequest =
          McpSchema.CallToolRequest.builder()
              .name(myTableTool.name())
              .arguments(Map.of("limit", 5))
              .build();

      var toolResult = client.callTool(callRequest);
      log.info("Tool call result: {}", toolResult);
      assertThat(toolResult).isNotNull();
      assertThat(toolResult.isError()).isFalse();
      assertThat(toolResult.content()).isNotNull();
    } finally {
      var logs = serverContainer.getLogs();
      log.info("Detailed MCP Validation Results:");
      System.out.println(logs);
    }
  }

  private String generateJwtToken() {
    var now = Instant.now();
    var expiration = now.plusSeconds(20);

    return Jwts.builder()
        .issuer("my-test-issuer")
        .audience()
        .add("my-test-audience")
        .and()
        .issuedAt(Date.from(now))
        .expiration(Date.from(expiration))
        .claim("val", 1)
        .claim("values", List.of(1, 2, 3))
        .signWith(
            new SecretKeySpec(
                "testSecretThatIsAtLeast256BitsLong32Chars".getBytes(UTF_8), "HmacSHA256"))
        .compact();
  }

  private String generateRS256JwtToken() {
    var now = Instant.now();
    var expiration = now.plusSeconds(20);

    // Generate an RSA key pair for RS256 algorithm (different from server's expected HS256)
    try {
      var keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(2048);
      var keyPair = keyPairGenerator.generateKeyPair();

      return Jwts.builder()
          .issuer("my-test-issuer")
          .audience()
          .add("my-test-audience")
          .and()
          .issuedAt(Date.from(now))
          .expiration(Date.from(expiration))
          .signWith(keyPair.getPrivate(), Jwts.SIG.RS256)
          .compact();
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate RSA key pair for test", e);
    }
  }

  private String getFullExceptionMessage(Throwable throwable) {
    var messages = new StringBuilder();
    var current = throwable;
    while (current != null) {
      if (current.getMessage() != null) {
        if (messages.length() > 0) {
          messages.append(" -> ");
        }
        messages.append(current.getMessage());
      }
      current = current.getCause();
    }
    return messages.toString();
  }
}
