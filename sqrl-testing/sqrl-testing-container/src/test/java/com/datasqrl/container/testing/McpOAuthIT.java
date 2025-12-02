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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
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
 * Integration tests for MCP OAuth authentication with Keycloak. Tests the full OAuth flow including
 * discovery endpoints and token validation.
 */
@Testcontainers
@Slf4j
class McpOAuthIT extends SqrlContainerTestBase {

  private static final String KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak:26.0";
  private static final String KEYCLOAK_ADMIN = "admin";
  private static final String KEYCLOAK_PASSWORD = "admin";
  private static final String REALM_NAME = "datasqrl";
  private static final String CLIENT_ID = "mcp-client";
  private static final String CLIENT_SECRET = "mcp-secret";
  private static final String TEST_USER = "testuser";
  private static final String TEST_PASSWORD = "testpassword";

  private static GenericContainer<?> keycloak;
  private static String keycloakInternalUrl;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  protected String getTestCaseName() {
    return "oauth-authorized";
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
    log.info("Keycloak started at internal URL: {}", keycloakInternalUrl);

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
    createClient(keycloakUrl, adminToken);
    createUser(keycloakUrl, adminToken);

    log.info("Keycloak realm '{}' configured successfully", REALM_NAME);
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
              "sslRequired": "none"
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
  private static void createClient(String keycloakUrl, String adminToken) {
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
  }

  @SneakyThrows
  private static void createUser(String keycloakUrl, String adminToken) {
    var usersUrl = keycloakUrl + "/admin/realms/" + REALM_NAME + "/users";

    var userConfig =
        String.format(
            """
            {
              "username": "%s",
              "enabled": true,
              "credentials": [{
                "type": "password",
                "value": "%s",
                "temporary": false
              }]
            }
            """,
            TEST_USER, TEST_PASSWORD);

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
  private String getUserToken() {
    var keycloakHost = "localhost:" + keycloak.getMappedPort(8080);
    var tokenUrl =
        "http://" + keycloakHost + "/realms/" + REALM_NAME + "/protocol/openid-connect/token";

    var request = new HttpPost(tokenUrl);
    request.setEntity(
        new StringEntity(
            String.format(
                "grant_type=password&client_id=%s&client_secret=%s&username=%s&password=%s",
                CLIENT_ID, CLIENT_SECRET, TEST_USER, TEST_PASSWORD),
            ContentType.APPLICATION_FORM_URLENCODED));

    var response = sharedHttpClient.execute(request);
    var body = EntityUtils.toString(response.getEntity());
    var json = objectMapper.readTree(body);

    return json.get("access_token").asText();
  }

  @Test
  @SneakyThrows
  void givenOAuthConfig_whenFetchDiscoveryEndpoint_thenReturnsProtectedResourceMetadata() {
    compileSqrlProject(
        testDir,
        c -> {
          c.withEnv("KEYCLOAK_ISSUER", keycloakInternalUrl + "/realms/" + REALM_NAME + "/");
        });

    startGraphQLServer(
        testDir,
        c -> {
          c.withEnv("KEYCLOAK_ISSUER", keycloakInternalUrl + "/realms/" + REALM_NAME + "/");
        });

    var discoveryUrl = getBaseUrl() + "/.well-known/oauth-protected-resource";
    log.info("Testing OAuth discovery endpoint: {}", discoveryUrl);

    var request = new HttpGet(discoveryUrl);
    var response = sharedHttpClient.execute(request);

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    var body = EntityUtils.toString(response.getEntity());
    log.info("Discovery response: {}", body);

    var json = objectMapper.readTree(body);

    assertThat(json.has("authorization_servers")).isTrue();
    assertThat(json.get("authorization_servers").isArray()).isTrue();
    assertThat(json.get("authorization_servers").get(0).asText())
        .contains("keycloak:8080/realms/" + REALM_NAME);

    assertThat(json.has("scopes_supported")).isTrue();
    assertThat(json.get("scopes_supported").isArray()).isTrue();

    assertThat(json.has("bearer_methods_supported")).isTrue();
    assertThat(json.get("bearer_methods_supported").get(0).asText()).isEqualTo("header");

    assertThat(json.has("resource")).isTrue();
  }

  @Test
  @SneakyThrows
  void givenOAuthConfig_whenMcpRequestWithoutToken_thenReturns401WithWWWAuthenticate() {
    compileSqrlProject(
        testDir,
        c -> {
          c.withEnv("KEYCLOAK_ISSUER", keycloakInternalUrl + "/realms/" + REALM_NAME + "/");
        });

    startGraphQLServer(
        testDir,
        c -> {
          c.withEnv("KEYCLOAK_ISSUER", keycloakInternalUrl + "/realms/" + REALM_NAME + "/");
        });

    var mcpUrl = getBaseUrl() + "/v1/mcp";
    log.info("Testing MCP endpoint without token: {}", mcpUrl);

    var request = new HttpPost(mcpUrl);
    request.setEntity(
        new StringEntity(
            "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"id\":1}",
            ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);

    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

    var wwwAuth = response.getFirstHeader("WWW-Authenticate");
    assertThat(wwwAuth).isNotNull();
    assertThat(wwwAuth.getValue()).contains("resource_metadata=");
    assertThat(wwwAuth.getValue()).contains(".well-known/oauth-protected-resource");
  }

  /** Compiles the SQRL project with environment variable support. */
  protected void compileSqrlProject(
      java.nio.file.Path workingDir, java.util.function.Consumer<GenericContainer<?>> customizer) {
    cmd = createCmdContainer(workingDir).withCommand("compile", "package.json");
    customizer.accept(cmd);
    cmd.start();

    org.awaitility.Awaitility.await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var exitCode = cmd.getCurrentContainerInfo().getState().getExitCodeLong();
    var logs = cmd.getLogs();
    if (exitCode != 0) {
      log.error("SQRL compilation failed with exit code {}\n{}", exitCode, logs);
      throw new ContainerError("SQRL compilation failed", exitCode, logs);
    }

    log.info("SQRL script compiled successfully");
  }

  /** Starts the GraphQL server with custom environment variables. */
  protected void startGraphQLServer(
      java.nio.file.Path workingDir, java.util.function.Consumer<GenericContainer<?>> customizer) {
    serverContainer = createServerContainer(workingDir);
    customizer.accept(serverContainer);
    serverContainer.start();
    log.info("HTTP server started on port {}", serverContainer.getMappedPort(HTTP_SERVER_PORT));
  }
}
