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

import static com.datasqrl.container.testing.KeycloakContainerExtension.issuer;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;

/**
 * Runs the {@code auth} block with OIDC discovery against Keycloak through the real server image.
 * Two realms are two issuers, each with its own signing keys; the server trusts realm alpha for
 * {@code sqrl-api-alpha} and realm beta for {@code sqrl-api-beta}.
 */
class AuthOidcContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("oauth-authorized-compile");

  @RegisterExtension
  static KeycloakContainerExtension keycloak = new KeycloakContainerExtension(sqrl);

  private static final String TYPENAME_QUERY = "{\"query\":\"query { __typename }\"}";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeAll
  static void createRealms() {
    keycloak.createRealm(
        realm(
            "alpha",
            client("app", "sqrl-api-alpha"),
            client("wrong-audience", "unrelated-api"),
            client("cross-audience", "sqrl-api-beta")));
    keycloak.createRealm(realm("beta", client("app", "sqrl-api-beta")));
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
          .containsExactly(issuer("alpha"), issuer("beta"));
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
            c.withEnv("KEYCLOAK_ISSUER_ALPHA", issuer("alpha"))
                .withEnv("KEYCLOAK_ISSUER_BETA", issuer("beta"));
    sqrl.compileSqrlProject("auth-oidc.json", env);
    sqrl.startGraphQLServer(env);
  }

  @SneakyThrows
  private int status(String token) {
    try (var response = sqrl.executeGraphQLQuery(TYPENAME_QUERY, token)) {
      return response.getStatusLine().getStatusCode();
    }
  }

  private static String realm(String realm, String... clients) {
    return """
        { "realm": "%s", "enabled": true, "sslRequired": "none", "clients": [%s] }
        """
        .formatted(realm, String.join(",", clients));
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

  private static String clientToken(String realm, String clientId) {
    return keycloak.token(
        realm,
        "grant_type=client_credentials&client_id=%s&client_secret=%s-secret"
            .formatted(clientId, clientId));
  }
}
