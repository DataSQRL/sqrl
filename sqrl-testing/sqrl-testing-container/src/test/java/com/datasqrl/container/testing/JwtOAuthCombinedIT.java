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
import static org.awaitility.Awaitility.await;

import io.jsonwebtoken.Jwts;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import javax.crypto.spec.SecretKeySpec;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for combined JWT and OAuth authentication. Tests that both authentication
 * methods work when configured together, specifically verifying that JWT claims are accessible.
 */
@Testcontainers
@Slf4j
class JwtOAuthCombinedIT {
  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("jwt-oauth-combined");

  @RegisterExtension
  static PostgresContainerExtension postgres =
      new PostgresContainerExtension(sqrl, JwtOAuthCombinedIT::executeStatements);

  @RegisterExtension
  static KeycloakContainerExtension keycloak = new KeycloakContainerExtension(sqrl);

  private static final String REALM_NAME = "datasqrl-jwt-oauth";
  private static final String CLIENT_ID = "jwt-oauth-client";
  private static final String CLIENT_SECRET = "jwt-oauth-secret";
  private static final String TEST_USER = "jwtoauthuser";
  private static final String TEST_PASSWORD = "testpassword";
  private static final int CLAIM_VAL = 5;

  private static void executeStatements(Statement stmt) throws SQLException {
    stmt.execute("CREATE TABLE IF NOT EXISTS \"MyTable\" (val INTEGER)");
    stmt.execute(
        "INSERT INTO \"MyTable\" (val) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
    log.info("Test tables created and populated successfully");
  }

  @BeforeAll
  static void createRealm() {
    keycloak.createRealm(
        """
        {
          "realm": "%s",
          "enabled": true,
          "sslRequired": "none",
          "registrationAllowed": false,
          "verifyEmail": false,
          "loginWithEmailAllowed": true,
          "clients": [{
            "clientId": "%s",
            "secret": "%s",
            "enabled": true,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "publicClient": false,
            "protocol": "openid-connect",
            "attributes": { "access.token.lifespan": "3600" },
            "protocolMappers": [{
              "name": "val-claim-mapper",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-hardcoded-claim-mapper",
              "consentRequired": false,
              "config": {
                "claim.name": "val",
                "claim.value": "%d",
                "jsonType.label": "long",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true"
              }
            }]
          }],
          "users": [{
            "username": "%s",
            "email": "%s@test.local",
            "firstName": "Test",
            "lastName": "User",
            "enabled": true,
            "emailVerified": true,
            "requiredActions": [],
            "credentials": [{ "type": "password", "value": "%s", "temporary": false }]
          }]
        }
        """
            .formatted(
                REALM_NAME,
                CLIENT_ID,
                CLIENT_SECRET,
                CLAIM_VAL,
                TEST_USER,
                TEST_USER,
                TEST_PASSWORD));
  }

  private String getKeycloakToken() {
    return keycloak.token(
        REALM_NAME,
        "grant_type=password&client_id=%s&client_secret=%s&username=%s&password=%s"
            .formatted(CLIENT_ID, CLIENT_SECRET, TEST_USER, TEST_PASSWORD));
  }

  private void compileAndStartServerWithKeycloak() {
    postgres.startPostgreSQLContainer();
    compileSqrlProjectWithKeycloak();
    startGraphQLServerWithKeycloak();
  }

  private void compileSqrlProjectWithKeycloak() {
    var cmd =
        sqrl.createCmdContainer()
            .withCommand("compile", "package.json")
            .withEnv("KEYCLOAK_ISSUER", KeycloakContainerExtension.issuer(REALM_NAME) + "/")
            .withEnv(
                "KEYCLOAK_EXTERNAL_URL", keycloak.getHostUrl() + "/realms/" + REALM_NAME + "/");
    cmd.start();

    await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var exitCode = cmd.getCurrentContainerInfo().getState().getExitCodeLong();
    var logs = cmd.getLogs();
    if (exitCode == null || exitCode != 0) {
      log.error("SQRL compilation failed with exit code {}\n{}", exitCode, logs);
      throw new ContainerError("SQRL compilation failed", exitCode, logs);
    }

    log.info("SQRL script compiled successfully with Keycloak config");
  }

  private void startGraphQLServerWithKeycloak() {
    var serverContainer =
        sqrl.createServerContainer()
            .withEnv("KEYCLOAK_ISSUER", KeycloakContainerExtension.issuer(REALM_NAME) + "/")
            .withEnv("KEYCLOAK_EXTERNAL_URL", keycloak.getHostUrl() + "/realms/" + REALM_NAME + "/")
            .withEnv("POSTGRES_HOST", "postgresql")
            .withEnv("POSTGRES_USERNAME", postgres.getPostgresql().getUsername())
            .withEnv("POSTGRES_PASSWORD", postgres.getPostgresql().getPassword())
            .withEnv("POSTGRES_DATABASE", postgres.getPostgresql().getDatabaseName())
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    serverContainer.start();
    log.info(
        "HTTP server started on port {}",
        serverContainer.getMappedPort(SqrlContainerExtension.HTTP_SERVER_PORT));
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenUnauthenticated_thenReturns401() {
    compileAndStartServerWithKeycloak();

    try (var response =
        sqrl.executeGraphQLQuery("{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}")) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);
    }
    log.info("Unauthenticated request correctly rejected with 401");
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenJwtWithClaim_thenSucceeds() {
    compileAndStartServerWithKeycloak();

    var jwtToken = generateHmacJwtToken(Map.of("val", CLAIM_VAL));
    try (var response =
        sqrl.executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", jwtToken)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      var responseBody = EntityUtils.toString(response.getEntity());
      assertThat(responseBody).contains("\"data\"");
      assertThat(responseBody).contains("\"AuthMyTable\"");
      assertThat(responseBody).contains("\"val\":" + CLAIM_VAL);
      log.info("JWT with claim succeeded, response: {}", responseBody);
    }
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenOAuthWithClaim_thenSucceeds() {
    compileAndStartServerWithKeycloak();

    var keycloakToken = getKeycloakToken();
    log.info("Obtained Keycloak token with 'val' claim");

    try (var response =
        sqrl.executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", keycloakToken)) {
      var statusCode = response.getStatusLine().getStatusCode();
      var responseBody = EntityUtils.toString(response.getEntity());
      log.info("OAuth response status: {}, body: {}", statusCode, responseBody);

      assertThat(statusCode).isEqualTo(200);
      assertThat(responseBody).contains("\"data\"");
      assertThat(responseBody).contains("\"AuthMyTable\"");
      assertThat(responseBody).contains("\"val\":" + CLAIM_VAL);
    }
    log.info("OAuth with claim succeeded");
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenBothTokensWithClaims_thenBothWork() {
    compileAndStartServerWithKeycloak();

    // Test JWT with claim
    var jwtToken = generateHmacJwtToken(Map.of("val", CLAIM_VAL));
    try (var jwtResponse =
        sqrl.executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", jwtToken)) {
      assertThat(jwtResponse.getStatusLine().getStatusCode()).isEqualTo(200);
      var jwtBody = EntityUtils.toString(jwtResponse.getEntity());
      assertThat(jwtBody).contains("\"val\":" + CLAIM_VAL);
    }
    log.info("JWT token with claim accepted");

    // Test OAuth with claim
    var keycloakToken = getKeycloakToken();
    try (var oauthResponse =
        sqrl.executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", keycloakToken)) {
      assertThat(oauthResponse.getStatusLine().getStatusCode()).isEqualTo(200);
      var oauthBody = EntityUtils.toString(oauthResponse.getEntity());
      assertThat(oauthBody).contains("\"val\":" + CLAIM_VAL);
    }
    log.info("OAuth token with claim accepted");

    log.info("Both JWT and OAuth tokens with claims work correctly");
  }

  private String generateHmacJwtToken(Map<String, Object> claims) {
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
}
