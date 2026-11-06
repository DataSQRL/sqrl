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

import static com.datasqrl.container.testing.McpAssertions.assertMcpAuthorizationStatus;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Runs the {@code auth} block with inline keys through the real server image. The package is
 * layered: {@code auth-base.json} trusts two issuers with {@code leeway: 0}, and {@code
 * auth-leeway-overlay.json} patches only {@code auth.leeway}. The issuer, audience and key pairing
 * itself is covered by {@code TrustedIssuerAuthTest}.
 */
class AuthInlineKeysContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("jwt-authorized");

  @RegisterExtension
  static PostgresContainerExtension postgres =
      new PostgresContainerExtension(sqrl, AuthInlineKeysContainerIT::executeStatements);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<String> PACKAGE_FILES =
      List.of("auth-base.json", "auth-leeway-overlay.json");
  private static final String TYPENAME_QUERY = "{\"query\":\"query { __typename }\"}";
  private static final String AUTH_QUERY =
      "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}";
  private static final String FIRST_ISSUER = "my-test-issuer";
  private static final String FIRST_AUDIENCE = "my-test-audience";
  private static final String FIRST_SECRET = "testSecretThatIsAtLeast256BitsLong32Chars";
  private static final String SECOND_ISSUER = "my-second-issuer";
  private static final String SECOND_AUDIENCE = "my-second-audience";
  private static final String SECOND_SECRET = "secondSecretThatIsAtLeast256BitsLong32Chars";

  private static void executeStatements(Statement stmt) throws SQLException {
    stmt.execute("CREATE TABLE IF NOT EXISTS \"MyTable\" (val INTEGER)");
    stmt.execute(
        "INSERT INTO \"MyTable\" (val) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
    stmt.execute("CREATE TABLE IF NOT EXISTS \"MyStringTable\" (val VARCHAR(1))");
    stmt.execute("INSERT INTO \"MyStringTable\" (val) VALUES ('a'), ('b'), ('c'), ('d')");
  }

  @Test
  @SneakyThrows
  void givenLayeredAuth_whenCompiled_thenOverlayPatchesLeewayAndKeepsBaseIssuers() {
    sqrl.compileSqrlProject(PACKAGE_FILES);

    var vertxConfig =
        MAPPER.readTree(
            Files.readString(sqrl.getTestDir().resolve("build/deploy/plan/vertx-config.json")));
    var auth = vertxConfig.get("auth");

    assertThat(auth.get("leeway").asInt()).isEqualTo(300);
    assertThat(auth.get("issuers").findValuesAsText("issuer"))
        .containsExactly(FIRST_ISSUER, SECOND_ISSUER);
    assertThat(vertxConfig.get("jwtAuth").isNull()).isTrue();
  }

  @Test
  @SneakyThrows
  void givenLayeredInlineIssuers_whenTokensPresented_thenBothIssuersAndOverlayLeewayApply() {
    sqrl.compileSqrlProject(PACKAGE_FILES);
    sqrl.startGraphQLServer();

    assertThat(status(null)).as("no token").isEqualTo(401);
    assertThat(status(token(FIRST_ISSUER, FIRST_AUDIENCE, FIRST_SECRET, 3600)))
        .as("first issuer")
        .isEqualTo(200);
    assertThat(status(token(SECOND_ISSUER, SECOND_AUDIENCE, SECOND_SECRET, 3600)))
        .as("second issuer")
        .isEqualTo(200);
    assertThat(status(token(SECOND_ISSUER, SECOND_AUDIENCE, FIRST_SECRET, 3600)))
        .as("second issuer signed with first key")
        .isEqualTo(401);
    assertThat(status(token(FIRST_ISSUER, FIRST_AUDIENCE, FIRST_SECRET, -120)))
        .as("expired 120s ago, within the overlay leeway of 300s")
        .isEqualTo(200);
    assertThat(status(token(FIRST_ISSUER, FIRST_AUDIENCE, FIRST_SECRET, -600)))
        .as("expired 600s ago")
        .isEqualTo(401);
  }

  @Test
  @SneakyThrows
  void givenInlineIssuer_whenAuthenticatedGraphQLAndMcp_thenClaimsReachQueries() {
    postgres.compileAndStartServerWithDatabase(PACKAGE_FILES);
    var token = token(SECOND_ISSUER, SECOND_AUDIENCE, SECOND_SECRET, 3600);

    try (var response = sqrl.executeGraphQLQuery(AUTH_QUERY, token)) {
      var body = EntityUtils.toString(response.getEntity());
      assertThat(response.getStatusLine().getStatusCode()).as(body).isEqualTo(200);
      assertThat(MAPPER.readTree(body).at("/data/AuthMyTable").findValuesAsText("val"))
          .containsExactly("1");
    }

    var mcpUrl = sqrl.getBaseUrl() + "/v1/mcp";
    var unauthenticated =
        McpClient.sync(
                HttpClientStreamableHttpTransport.builder(mcpUrl).endpoint("/v1/mcp").build())
            .requestTimeout(Duration.ofSeconds(10))
            .build();
    assertThatThrownBy(unauthenticated::initialize)
        .satisfies(ex -> assertMcpAuthorizationStatus(ex, 401));
    unauthenticated.close();

    var transport =
        HttpClientStreamableHttpTransport.builder(mcpUrl)
            .addHttpRequestCustomizer(
                (requestBuilder, method, uri, body, context) ->
                    requestBuilder.header("Authorization", "Bearer " + token))
            .endpoint("/v1/mcp")
            .build();
    try (var client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build()) {
      client.initialize();
      var result =
          client.callTool(
              McpSchema.CallToolRequest.builder("GetAuthMyTable")
                  .arguments(Map.of("limit", 5))
                  .build());
      assertThat(result.isError()).isFalse();
    }

    var restRequest = new HttpGet(sqrl.getBaseUrl() + "/v1/rest/queries/AuthMyTable?limit=5");
    restRequest.setHeader("Authorization", "Bearer " + token);
    try (var response = sqrl.getHttpClient().execute(restRequest)) {
      var body = EntityUtils.toString(response.getEntity());
      assertThat(response.getStatusLine().getStatusCode()).as(body).isEqualTo(200);
      assertThat(MAPPER.readTree(body).at("/data").findValuesAsText("val")).containsExactly("1");
    }
  }

  @SneakyThrows
  private int status(String token) {
    try (var response = sqrl.executeGraphQLQuery(TYPENAME_QUERY, token)) {
      return response.getStatusLine().getStatusCode();
    }
  }

  private static String token(String issuer, String audience, String secret, long expiresIn) {
    var now = Instant.now();
    return Jwts.builder()
        .issuer(issuer)
        .audience()
        .add(audience)
        .and()
        .issuedAt(Date.from(now.minusSeconds(expiresIn < 0 ? 3600 : 0)))
        .expiration(Date.from(now.plusSeconds(expiresIn)))
        .claim("val", 1)
        .signWith(new SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"))
        .compact();
  }
}
