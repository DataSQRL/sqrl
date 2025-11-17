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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import feign.Feign;
import feign.FeignException;
import feign.Headers;
import feign.Param;
import feign.RequestLine;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import io.jsonwebtoken.Jwts;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import java.security.KeyPairGenerator;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

@Slf4j
public class JwtContainerIT extends SqrlWithPostgresContainerTestBase {

  // Response types for REST endpoints
  static class MyTableItem {
    public int val;
  }

  static class MyTableResponse {
    public List<MyTableItem> data;
  }

  // Feign client for testing REST endpoints
  interface RestClient {
    @RequestLine("GET /v1/rest/queries/AuthMyTable?limit={limit}")
    MyTableResponse getAuthMyTable(@Param("limit") int limit);

    @RequestLine("GET /v1/rest/queries/AuthMyTable?limit={limit}")
    @Headers("Authorization: Bearer {token}")
    MyTableResponse getAuthMyTableWithAuth(@Param("token") String token, @Param("limit") int limit);
  }

  @Override
  protected String getTestCaseName() {
    return "jwt-authorized";
  }

  @Override
  protected void executeStatements(Statement stmt) throws SQLException {
    // Create and populate tables as defined in jwt-authorized-base.sqrl

    // Create MyTable and populate with values 1-10
    stmt.execute("CREATE TABLE IF NOT EXISTS \"MyTable\" (val INTEGER)");
    stmt.execute(
        "INSERT INTO \"MyTable\" (val) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");

    // Create MyStringTable and populate with 'a', 'b', 'c', 'd'
    stmt.execute("CREATE TABLE IF NOT EXISTS \"MyStringTable\" (val VARCHAR(1))");
    stmt.execute("INSERT INTO \"MyStringTable\" (val) VALUES ('a'), ('b'), ('c'), ('d')");

    log.info("Test tables created and populated successfully");
  }

  @Test
  @SneakyThrows
  void givenJwt_whenUnauthenticatedGraphQL_thenReturns401() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);
  }

  @Test
  @SneakyThrows
  void givenJwt_whenAuthenticatedGraphQL_thenSucceeds() {
    compileAndStartServerWithDatabase("jwt-authorized-base.sqrl", testDir);

    var response =
        executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", generateJwtToken());

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    var responseBody = EntityUtils.toString(response.getEntity());
    assertThat(responseBody).contains("\"data\"");
    assertThat(responseBody).contains("\"AuthMyTable\"");
  }

  @Test
  @SneakyThrows
  void givenJwt_whenBadToken_thenReturns401() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    // Generate token with RS256 algorithm while server expects HS256
    var response =
        executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", generateRS256JwtToken());

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
  void givenJwt_whenCorruptedToken_thenReturns401() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    // Generate token with RS256 algorithm while server expects HS256
    var response =
        executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", "dummy-invalid-jwt");

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
  void givenJwt_whenMissingRequiredClaims_thenReturns403() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    try {
      // Generate valid JWT token but with empty claims (missing required claims)
      var response =
          executeGraphQLQuery(
              "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}",
              generateJwtToken(Map.of()));

      // Valid token but missing required claims should return 403 Forbidden
      var responseBody = EntityUtils.toString(response.getEntity());
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);
      assertThat(responseBody).contains("\"errors\"");
    } finally {
      var logs = serverContainer.getLogs();
      log.info("Detailed server logs:");
      System.out.println(logs);
    }
  }

  @Test
  @SneakyThrows
  void givenJwt_whenUnauthenticatedMcp_thenFails() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    var mcpUrl =
        String.format(
            "http://localhost:%d/v1/mcp", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    log.info("Testing MCP endpoint without JWT: {}", mcpUrl);

    // Create MCP client without authentication
    var transport = HttpClientStreamableHttpTransport.builder(mcpUrl).endpoint("/v1/mcp").build();
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
  void givenJwt_whenAuthenticatedMcp_thenSucceeds() {
    compileAndStartServerWithDatabase("jwt-authorized-base.sqrl", testDir);

    var mcpUrl =
        String.format(
            "http://localhost:%d/v1/mcp", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    var jwtToken = generateJwtToken();
    log.info("Testing MCP endpoint with valid JWT: {}", mcpUrl);

    // Create MCP client with JWT authentication
    var transport =
        HttpClientStreamableHttpTransport.builder(mcpUrl)
            .customizeRequest(r -> r.header("Authorization", "Bearer " + jwtToken))
            .endpoint("/v1/mcp")
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
              .filter(tool -> "GetAuthMyTable".equals(tool.name()))
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

  @Test
  @SneakyThrows
  void givenJwt_whenMissingRequiredClaimsMcp_thenFails() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    var mcpUrl =
        String.format(
            "http://localhost:%d/v1/mcp", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    var jwtToken = generateJwtToken(Map.of());
    log.info("Testing MCP endpoint with JWT missing claims: {}", mcpUrl);

    // Create MCP client with JWT authentication but missing required claims
    var transport =
        HttpClientStreamableHttpTransport.builder(mcpUrl)
            .customizeRequest(r -> r.header("Authorization", "Bearer " + jwtToken))
            .endpoint("/v1/mcp")
            .build();

    try (var client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build(); ) {
      // Should succeed with proper JWT - authentication passes
      client.initialize();

      var tools = client.listTools();

      log.info("Successfully listed {} tools", tools.tools().size());
      assertThat(tools).isNotNull();
      assertThat(tools.tools()).isNotNull();
      assertThat(tools.tools()).isNotEmpty();

      // Test calling a tool that requires auth metadata - should fail with Forbidden
      var myTableTool =
          tools.tools().stream()
              .filter(tool -> "GetAuthMyTable".equals(tool.name()))
              .findFirst()
              .orElseThrow(() -> new RuntimeException("GetAuthMyTable tool not found"));
      log.info("Testing tool call: {}", myTableTool.name());

      var callRequest =
          McpSchema.CallToolRequest.builder()
              .name(myTableTool.name())
              .arguments(Map.of("limit", 5))
              .build();

      // Should throw exception when calling tool with missing required claims
      assertThatThrownBy(() -> client.callTool(callRequest))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Forbidden");
    }
  }

  @Test
  @SneakyThrows
  void givenJwt_whenUnauthenticatedRest_thenReturns401() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    var restUrl =
        String.format("http://localhost:%d", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    log.info("Testing REST endpoint without auth: {}", restUrl);

    var restClient =
        Feign.builder()
            .encoder(new JacksonEncoder())
            .decoder(new JacksonDecoder())
            .target(RestClient.class, restUrl);

    // AuthMyTable endpoint requires authentication
    assertThatThrownBy(() -> restClient.getAuthMyTable(5))
        .isInstanceOf(FeignException.Unauthorized.class)
        .hasMessageContaining("JWT auth failed");
  }

  @Test
  @SneakyThrows
  void givenJwt_whenAuthenticatedRest_thenSucceeds() {
    compileAndStartServerWithDatabase("jwt-authorized-base.sqrl", testDir);

    var restUrl =
        String.format("http://localhost:%d", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    var jwtToken = generateJwtToken();
    log.info("Testing REST endpoint with valid JWT: {}", restUrl);

    var restClient =
        Feign.builder()
            .encoder(new JacksonEncoder())
            .decoder(new JacksonDecoder())
            .target(RestClient.class, restUrl);

    // REST endpoint should work with valid JWT
    var response = restClient.getAuthMyTableWithAuth(jwtToken, 5);
    log.info("REST endpoint response with JWT: {}", response);

    assertThat(response).isNotNull();
    assertThat(response.data).isNotNull();
    assertThat(response.data).isNotEmpty();
    assertThat(response.data).allSatisfy(item -> assertThat(item.val).isPositive());
  }

  @Test
  @SneakyThrows
  void givenJwt_whenMissingRequiredClaimsRest_thenReturns403() {
    compileAndStartServer("jwt-authorized-base.sqrl", testDir);

    var restUrl =
        String.format("http://localhost:%d", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    var jwtToken = generateJwtToken(Map.of());
    log.info("Testing REST endpoint with JWT missing claims: {}", restUrl);

    try {
      var restClient =
          Feign.builder()
              .encoder(new JacksonEncoder())
              .decoder(new JacksonDecoder())
              .target(RestClient.class, restUrl);

      // Valid token but missing required claims should return 403 Forbidden
      assertThatThrownBy(() -> restClient.getAuthMyTableWithAuth(jwtToken, 5))
          .isInstanceOf(FeignException.Forbidden.class);
    } finally {
      var logs = serverContainer.getLogs();
      log.info("Detailed MCP Validation Results:");
      System.out.println(logs);
    }
  }

  private String generateJwtToken() {
    return generateJwtToken(Map.of("val", 1, "values", List.of(1, 2, 3)));
  }

  private String generateJwtToken(Map<String, Object> claims) {
    var now = Instant.now();
    var expiration = now.plus(1, ChronoUnit.HOURS);

    var builder =
        Jwts.builder()
            .issuer("my-test-issuer")
            .audience()
            .add("my-test-audience")
            .and()
            .issuedAt(Date.from(now))
            .expiration(Date.from(expiration));

    claims.forEach(builder::claim);

    return builder
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
