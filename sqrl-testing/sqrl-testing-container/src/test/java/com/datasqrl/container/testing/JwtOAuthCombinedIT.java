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

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for combined JWT and OAuth authentication. Tests that both authentication
 * methods work when configured together, specifically verifying that JWT claims are accessible.
 */
@Testcontainers
@Slf4j
class JwtOAuthCombinedIT extends SqrlWithPostgresContainerTestBase {

  private static final String KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak:26.0";
  private static final String KEYCLOAK_ADMIN = "admin";
  private static final String KEYCLOAK_PASSWORD = "admin";
  private static final String REALM_NAME = "datasqrl-jwt-oauth";
  private static final String CLIENT_ID = "jwt-oauth-client";
  private static final String CLIENT_SECRET = "jwt-oauth-secret";
  private static final String TEST_USER = "jwtoauthuser";
  private static final String TEST_PASSWORD = "testpassword";
  private static final int CLAIM_VAL = 5;

  private static GenericContainer<?> keycloak;
  private static String keycloakInternalUrl;
  private static String keycloakExternalUrl;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  protected String getTestCaseName() {
    return "jwt-oauth-combined";
  }

  @Override
  protected void executeStatements(Statement stmt) throws SQLException {
    stmt.execute("CREATE TABLE IF NOT EXISTS \"MyTable\" (val INTEGER)");
    stmt.execute(
        "INSERT INTO \"MyTable\" (val) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
    log.info("Test tables created and populated successfully");
  }

  @BeforeAll
  static void startKeycloak() {
    keycloak =
        new GenericContainer<>(DockerImageName.parse(KEYCLOAK_IMAGE))
            .withNetwork(sharedNetwork)
            .withNetworkAliases("keycloak")
            .withExposedPorts(8080)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", KEYCLOAK_ADMIN)
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", KEYCLOAK_PASSWORD)
            .withEnv("KC_HTTP_RELATIVE_PATH", "/")
            .withCommand("start-dev")
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("keycloak"))
            .waitingFor(
                Wait.forHttp("/realms/master")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    keycloak.start();

    keycloakInternalUrl = "http://keycloak:8080";
    keycloakExternalUrl = "http://localhost:" + keycloak.getMappedPort(8080);
    log.info(
        "Keycloak started - internal: {}, external: {}", keycloakInternalUrl, keycloakExternalUrl);

    configureKeycloakRealm();
  }

  @AfterAll
  static void stopKeycloak() {
    if (keycloak != null) {
      keycloak.stop();
    }
  }

  @SneakyThrows
  private static void configureKeycloakRealm() {
    var keycloakHost = "localhost:" + keycloak.getMappedPort(8080);
    var keycloakUrl = "http://" + keycloakHost;

    log.info("Configuring Keycloak realm at: {}", keycloakUrl);

    var adminToken = getAdminToken(keycloakUrl);

    createRealm(keycloakUrl, adminToken);
    var clientUuid = createClient(keycloakUrl, adminToken);
    addClaimMapper(keycloakUrl, adminToken, clientUuid);
    createUser(keycloakUrl, adminToken);

    log.info("Keycloak realm '{}' configured successfully with 'val' claim mapper", REALM_NAME);
  }

  @SneakyThrows
  private static String getAdminToken(String keycloakUrl) {
    var tokenUrl = keycloakUrl + "/realms/master/protocol/openid-connect/token";

    var request = new HttpPost(tokenUrl);
    request.setEntity(
        new StringEntity(
            "grant_type=password&client_id=admin-cli&username="
                + KEYCLOAK_ADMIN
                + "&password="
                + KEYCLOAK_PASSWORD,
            ContentType.APPLICATION_FORM_URLENCODED));

    var response = sharedHttpClient.execute(request);
    var body = EntityUtils.toString(response.getEntity());
    var json = new ObjectMapper().readTree(body);

    return json.get("access_token").asText();
  }

  @SneakyThrows
  private static void createRealm(String keycloakUrl, String adminToken) {
    var realmUrl = keycloakUrl + "/admin/realms";

    var realmConfig =
        String.format(
            """
            {
              "realm": "%s",
              "enabled": true,
              "sslRequired": "none",
              "registrationAllowed": false,
              "verifyEmail": false,
              "loginWithEmailAllowed": true
            }
            """,
            REALM_NAME);

    var request = new HttpPost(realmUrl);
    request.setHeader("Authorization", "Bearer " + adminToken);
    request.setEntity(new StringEntity(realmConfig, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);
    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 409) {
      log.info("Realm '{}' already exists", REALM_NAME);
    } else if (statusCode != 201) {
      var body = EntityUtils.toString(response.getEntity());
      throw new RuntimeException("Failed to create realm: " + statusCode + " - " + body);
    }
  }

  @SneakyThrows
  private static String createClient(String keycloakUrl, String adminToken) {
    var clientUrl = keycloakUrl + "/admin/realms/" + REALM_NAME + "/clients";

    var clientConfig =
        String.format(
            """
            {
              "clientId": "%s",
              "secret": "%s",
              "enabled": true,
              "directAccessGrantsEnabled": true,
              "serviceAccountsEnabled": true,
              "publicClient": false,
              "protocol": "openid-connect",
              "attributes": {
                "access.token.lifespan": "3600"
              }
            }
            """,
            CLIENT_ID, CLIENT_SECRET);

    var request = new HttpPost(clientUrl);
    request.setHeader("Authorization", "Bearer " + adminToken);
    request.setEntity(new StringEntity(clientConfig, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);
    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 409) {
      log.info("Client '{}' already exists", CLIENT_ID);
    } else if (statusCode != 201) {
      var body = EntityUtils.toString(response.getEntity());
      throw new RuntimeException("Failed to create client: " + statusCode + " - " + body);
    }

    return getClientUuid(keycloakUrl, adminToken);
  }

  @SneakyThrows
  private static String getClientUuid(String keycloakUrl, String adminToken) {
    var clientUrl = keycloakUrl + "/admin/realms/" + REALM_NAME + "/clients?clientId=" + CLIENT_ID;

    var request = new HttpGet(clientUrl);
    request.setHeader("Authorization", "Bearer " + adminToken);

    var response = sharedHttpClient.execute(request);
    var body = EntityUtils.toString(response.getEntity());
    var json = new ObjectMapper().readTree(body);

    return json.get(0).get("id").asText();
  }

  @SneakyThrows
  private static void addClaimMapper(String keycloakUrl, String adminToken, String clientUuid) {
    var mapperUrl =
        keycloakUrl
            + "/admin/realms/"
            + REALM_NAME
            + "/clients/"
            + clientUuid
            + "/protocol-mappers/models";

    var mapperConfig =
        String.format(
            """
            {
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
            }
            """,
            CLAIM_VAL);

    var request = new HttpPost(mapperUrl);
    request.setHeader("Authorization", "Bearer " + adminToken);
    request.setEntity(new StringEntity(mapperConfig, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);
    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 409) {
      log.info("Claim mapper already exists");
    } else if (statusCode != 201) {
      var body = EntityUtils.toString(response.getEntity());
      throw new RuntimeException("Failed to create claim mapper: " + statusCode + " - " + body);
    } else {
      log.info("Added 'val' claim mapper with value {} to client", CLAIM_VAL);
    }
  }

  @SneakyThrows
  private static void createUser(String keycloakUrl, String adminToken) {
    var usersUrl = keycloakUrl + "/admin/realms/" + REALM_NAME + "/users";

    var userConfig =
        String.format(
            """
            {
              "username": "%s",
              "email": "%s@test.local",
              "firstName": "Test",
              "lastName": "User",
              "enabled": true,
              "emailVerified": true,
              "requiredActions": [],
              "credentials": [{
                "type": "password",
                "value": "%s",
                "temporary": false
              }]
            }
            """,
            TEST_USER, TEST_USER, TEST_PASSWORD);

    var request = new HttpPost(usersUrl);
    request.setHeader("Authorization", "Bearer " + adminToken);
    request.setEntity(new StringEntity(userConfig, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);
    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 409) {
      log.info("User '{}' already exists", TEST_USER);
    } else if (statusCode != 201) {
      var body = EntityUtils.toString(response.getEntity());
      throw new RuntimeException("Failed to create user: " + statusCode + " - " + body);
    }
  }

  @SneakyThrows
  private String getKeycloakToken() {
    var keycloakHost = "localhost:" + keycloak.getMappedPort(8080);
    var tokenUrl =
        "http://" + keycloakHost + "/realms/" + REALM_NAME + "/protocol/openid-connect/token";

    log.info("Requesting token from: {}", tokenUrl);

    var request = new HttpPost(tokenUrl);
    request.setEntity(
        new StringEntity(
            String.format(
                "grant_type=password&client_id=%s&client_secret=%s&username=%s&password=%s",
                CLIENT_ID, CLIENT_SECRET, TEST_USER, TEST_PASSWORD),
            ContentType.APPLICATION_FORM_URLENCODED));

    var response = sharedHttpClient.execute(request);
    var body = EntityUtils.toString(response.getEntity());
    log.info("Token response status: {}", response.getStatusLine().getStatusCode());

    var json = objectMapper.readTree(body);
    if (json.has("error")) {
      throw new RuntimeException(
          "Failed to get token: "
              + json.get("error").asText()
              + " - "
              + json.path("error_description").asText());
    }

    return json.get("access_token").asText();
  }

  private void compileAndStartServerWithKeycloak() {
    startPostgreSQLContainer();
    compileSqrlProjectWithKeycloak();
    startGraphQLServerWithKeycloak();
  }

  private void compileSqrlProjectWithKeycloak() {
    cmd =
        createCmdContainer(testDir)
            .withCommand("compile", "package.json")
            .withEnv("KEYCLOAK_ISSUER", keycloakInternalUrl + "/realms/" + REALM_NAME + "/")
            .withEnv("KEYCLOAK_EXTERNAL_URL", keycloakExternalUrl + "/realms/" + REALM_NAME + "/");
    cmd.start();

    org.awaitility.Awaitility.await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var exitCode = cmd.getCurrentContainerInfo().getState().getExitCodeLong();
    var logs = cmd.getLogs();
    if (exitCode != 0) {
      log.error("SQRL compilation failed with exit code {}\n{}", exitCode, logs);
      throw new ContainerError("SQRL compilation failed", exitCode, logs);
    }

    log.info("SQRL script compiled successfully with Keycloak config");
  }

  private void startGraphQLServerWithKeycloak() {
    serverContainer =
        createServerContainer(testDir)
            .withEnv("KEYCLOAK_ISSUER", keycloakInternalUrl + "/realms/" + REALM_NAME + "/")
            .withEnv("KEYCLOAK_EXTERNAL_URL", keycloakExternalUrl + "/realms/" + REALM_NAME + "/")
            .withEnv("POSTGRES_HOST", "postgresql")
            .withEnv("POSTGRES_USERNAME", postgresql.getUsername())
            .withEnv("POSTGRES_PASSWORD", postgresql.getPassword())
            .withEnv("POSTGRES_DATABASE", postgresql.getDatabaseName())
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    serverContainer.start();
    log.info("HTTP server started on port {}", serverContainer.getMappedPort(HTTP_SERVER_PORT));
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenUnauthenticated_thenReturns401() {
    compileAndStartServerWithKeycloak();

    var response = executeGraphQLQuery("{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}");

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);
    log.info("Unauthenticated request correctly rejected with 401");
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenJwtWithClaim_thenSucceeds() {
    compileAndStartServerWithKeycloak();

    var jwtToken = generateHmacJwtToken(Map.of("val", CLAIM_VAL));
    var response =
        executeGraphQLQuery("{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", jwtToken);

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    var responseBody = EntityUtils.toString(response.getEntity());
    assertThat(responseBody).contains("\"data\"");
    assertThat(responseBody).contains("\"AuthMyTable\"");
    assertThat(responseBody).contains("\"val\":" + CLAIM_VAL);
    log.info("JWT with claim succeeded, response: {}", responseBody);
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenOAuthWithClaim_thenSucceeds() {
    compileAndStartServerWithKeycloak();

    var keycloakToken = getKeycloakToken();
    log.info("Obtained Keycloak token with 'val' claim");

    var response =
        executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", keycloakToken);

    var responseBody = EntityUtils.toString(response.getEntity());
    log.info(
        "OAuth response status: {}, body: {}",
        response.getStatusLine().getStatusCode(),
        responseBody);

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(responseBody).contains("\"data\"");
    assertThat(responseBody).contains("\"AuthMyTable\"");
    assertThat(responseBody).contains("\"val\":" + CLAIM_VAL);
    log.info("OAuth with claim succeeded");
  }

  @Test
  @SneakyThrows
  void givenCombinedAuth_whenBothTokensWithClaims_thenBothWork() {
    compileAndStartServerWithKeycloak();

    // Test JWT with claim
    var jwtToken = generateHmacJwtToken(Map.of("val", CLAIM_VAL));
    var jwtResponse =
        executeGraphQLQuery("{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", jwtToken);
    assertThat(jwtResponse.getStatusLine().getStatusCode()).isEqualTo(200);
    var jwtBody = EntityUtils.toString(jwtResponse.getEntity());
    assertThat(jwtBody).contains("\"val\":" + CLAIM_VAL);
    log.info("JWT token with claim accepted");

    // Test OAuth with claim
    var keycloakToken = getKeycloakToken();
    var oauthResponse =
        executeGraphQLQuery(
            "{\"query\":\"query { AuthMyTable(limit: 5) { val } }\"}", keycloakToken);
    assertThat(oauthResponse.getStatusLine().getStatusCode()).isEqualTo(200);
    var oauthBody = EntityUtils.toString(oauthResponse.getEntity());
    assertThat(oauthBody).contains("\"val\":" + CLAIM_VAL);
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
