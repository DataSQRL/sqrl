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
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import java.time.Duration;
import java.util.function.Consumer;
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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the {@code auth} block with OIDC discovery against Keycloak through the real server image.
 * Two realms are two issuers, each with its own signing keys; the server trusts realm alpha for
 * {@code sqrl-api-alpha} and realm beta for {@code sqrl-api-beta}.
 */
@Slf4j
class AuthOidcContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("oauth-authorized-compile");

  private static final String KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak:26.0";
  private static final String KEYCLOAK_URL = "http://keycloak:8080";
  private static final String ISSUER_ALPHA = KEYCLOAK_URL + "/realms/alpha";
  private static final String ISSUER_BETA = KEYCLOAK_URL + "/realms/beta";
  private static final String TYPENAME_QUERY = "{\"query\":\"query { __typename }\"}";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static GenericContainer<?> keycloak;
  private static String keycloakHostUrl;

  @BeforeAll
  @SneakyThrows
  static void startKeycloak() {
    keycloak =
        new GenericContainer<>(DockerImageName.parse(KEYCLOAK_IMAGE))
            .withNetwork(sqrl.getNetwork())
            .withNetworkAliases("keycloak")
            .withExposedPorts(8080)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
            .withEnv("KC_HOSTNAME", KEYCLOAK_URL)
            .withCommand("start-dev")
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("keycloak"))
            .waitingFor(
                Wait.forHttp("/realms/master")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    keycloak.start();
    keycloakHostUrl = "http://localhost:" + keycloak.getMappedPort(8080);

    var adminToken = adminToken();
    createRealm(
        adminToken,
        "alpha",
        client("app", "sqrl-api-alpha"),
        client("wrong-audience", "unrelated-api"),
        client("cross-audience", "sqrl-api-beta"));
    createRealm(adminToken, "beta", client("app", "sqrl-api-beta"));
  }

  @AfterAll
  static void stopKeycloak() {
    if (keycloak != null) {
      keycloak.stop();
    }
  }

  @Test
  @SneakyThrows
  void givenTwoDiscoveredIssuers_whenTokensPresented_thenOnlyPairedIssuerAndAudienceAccepted() {
    startServer();

    assertThat(status(null)).as("no token").isEqualTo(401);
    assertThat(status(clientToken("alpha", "app"))).as("alpha, own audience").isEqualTo(200);
    assertThat(status(clientToken("beta", "app"))).as("beta, own audience").isEqualTo(200);
    assertThat(status(clientToken("alpha", "wrong-audience")))
        .as("alpha, unrelated audience")
        .isEqualTo(401);
    assertThat(status(clientToken("alpha", "cross-audience")))
        .as("alpha, beta's audience")
        .isEqualTo(401);
  }

  @Test
  @SneakyThrows
  void givenDiscoveredIssuers_whenMcpClientDiscovers_thenIssuersAdvertisedAndTokenAccepted() {
    startServer();

    try (var response =
        sqrl.getHttpClient()
            .execute(new HttpGet(sqrl.getBaseUrl() + "/.well-known/oauth-protected-resource"))) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      var metadata = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
      assertThat(MAPPER.convertValue(metadata.get("authorization_servers"), String[].class))
          .containsExactly(ISSUER_ALPHA, ISSUER_BETA);
      assertThat(metadata.get("resource").asText()).endsWith("/v1/mcp");
    }

    var unauthenticated = new HttpPost(sqrl.getBaseUrl() + "/v1/mcp");
    unauthenticated.setEntity(
        new StringEntity(
            "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"id\":1}",
            ContentType.APPLICATION_JSON));
    try (var response = sqrl.getHttpClient().execute(unauthenticated)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);
      assertThat(response.getFirstHeader("WWW-Authenticate").getValue())
          .isEqualTo(
              "Bearer resource_metadata=\""
                  + sqrl.getBaseUrl()
                  + "/.well-known/oauth-protected-resource\"");
    }

    var token = clientToken("beta", "app");
    var transport =
        HttpClientStreamableHttpTransport.builder(sqrl.getBaseUrl() + "/v1/mcp")
            .addHttpRequestCustomizer(
                (requestBuilder, method, uri, body, context) ->
                    requestBuilder.header("Authorization", "Bearer " + token))
            .endpoint("/v1/mcp")
            .build();
    try (var client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build()) {
      client.initialize();
      assertThat(client.listTools().tools()).isNotEmpty();
    }
  }

  private void startServer() {
    Consumer<GenericContainer<?>> env =
        c ->
            c.withEnv("KEYCLOAK_ISSUER_ALPHA", ISSUER_ALPHA)
                .withEnv("KEYCLOAK_ISSUER_BETA", ISSUER_BETA);
    sqrl.compileSqrlProject("package-auth.json", env);
    sqrl.startGraphQLServer(env);
  }

  @SneakyThrows
  private int status(String token) {
    try (var response = sqrl.executeGraphQLQuery(TYPENAME_QUERY, token)) {
      return response.getStatusLine().getStatusCode();
    }
  }

  private static String client(String clientId, String audience) {
    return """
        {
          "clientId": "%s",
          "secret": "%s-secret",
          "enabled": true,
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "standardFlowEnabled": false,
          "protocol": "openid-connect",
          "protocolMappers": [{
            "name": "audience",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
            "config": {
              "included.custom.audience": "%s",
              "access.token.claim": "true",
              "id.token.claim": "false"
            }
          }]
        }
        """
        .formatted(clientId, clientId, audience);
  }

  @SneakyThrows
  private static void createRealm(String adminToken, String realm, String... clients) {
    var request = new HttpPost(keycloakHostUrl + "/admin/realms");
    request.setHeader("Authorization", "Bearer " + adminToken);
    request.setEntity(
        new StringEntity(
            """
            { "realm": "%s", "enabled": true, "sslRequired": "none", "clients": [%s] }
            """
                .formatted(realm, String.join(",", clients)),
            ContentType.APPLICATION_JSON));
    try (var response = sqrl.getHttpClient().execute(request)) {
      assertThat(response.getStatusLine().getStatusCode())
          .as(EntityUtils.toString(response.getEntity()))
          .isEqualTo(201);
    }
  }

  private static String adminToken() {
    return token("master", "grant_type=password&client_id=admin-cli&username=admin&password=admin");
  }

  private static String clientToken(String realm, String clientId) {
    return token(
        realm,
        "grant_type=client_credentials&client_id=%s&client_secret=%s-secret"
            .formatted(clientId, clientId));
  }

  @SneakyThrows
  private static String token(String realm, String form) {
    var request =
        new HttpPost(keycloakHostUrl + "/realms/" + realm + "/protocol/openid-connect/token");
    request.setEntity(new StringEntity(form, ContentType.APPLICATION_FORM_URLENCODED));
    try (var response = sqrl.getHttpClient().execute(request)) {
      var body = EntityUtils.toString(response.getEntity());
      assertThat(response.getStatusLine().getStatusCode()).as(body).isEqualTo(200);
      return MAPPER.readTree(body).get("access_token").asText();
    }
  }
}
