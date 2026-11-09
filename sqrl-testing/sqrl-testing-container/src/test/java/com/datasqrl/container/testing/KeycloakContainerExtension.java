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
import java.util.Objects;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts a Keycloak dev server on the network of a {@link SqrlContainerExtension}, for the whole
 * test class. Other containers reach it at {@link #URL}, which is also the issuer base of every
 * token it signs, also when the test fetches the token through {@link #getHostUrl()}. The {@link
 * SqrlContainerExtension} must run first, and JUnit does not order these extensions by declaration:
 * use {@code @Order} if this one fails because the network is missing. Create realms in a
 * {@code @BeforeAll} method.
 */
@Slf4j
public class KeycloakContainerExtension implements BeforeAllCallback, AfterAllCallback {

  public static final String URL = "http://keycloak:8080";

  private static final String IMAGE = "quay.io/keycloak/keycloak:26.0";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SqrlContainerExtension sqrl;
  private GenericContainer<?> keycloak;

  public KeycloakContainerExtension(SqrlContainerExtension sqrl) {
    this.sqrl = Objects.requireNonNull(sqrl);
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    keycloak =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withNetwork(
                Objects.requireNonNull(
                    sqrl.getNetwork(), "SqrlContainerExtension must run before Keycloak starts"))
            .withNetworkAliases("keycloak")
            .withExposedPorts(8080)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
            .withEnv("KC_HOSTNAME", URL)
            .withCommand("start-dev")
            .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("keycloak"))
            .waitingFor(
                Wait.forHttp("/realms/master")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    keycloak.start();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (keycloak != null) {
      keycloak.stop();
      keycloak = null;
    }
  }

  /** The Keycloak URL as seen from the test JVM. */
  public String getHostUrl() {
    return "http://localhost:" + keycloak.getMappedPort(8080);
  }

  /** The issuer of the given realm, as it appears in the tokens' {@code iss} claim. */
  public static String issuer(String realm) {
    return URL + "/realms/" + realm;
  }

  /**
   * Creates a realm from its JSON representation, which can also list its {@code clients} and
   * {@code users}.
   */
  @SneakyThrows
  public void createRealm(String realmJson) {
    var request = new HttpPost(getHostUrl() + "/admin/realms");
    request.setHeader(
        "Authorization",
        "Bearer "
            + token(
                "master", "grant_type=password&client_id=admin-cli&username=admin&password=admin"));
    request.setEntity(new StringEntity(realmJson, ContentType.APPLICATION_JSON));
    try (var response = sqrl.getHttpClient().execute(request)) {
      assertThat(response.getStatusLine().getStatusCode())
          .as(EntityUtils.toString(response.getEntity()))
          .isEqualTo(201);
    }
  }

  /** Requests an access token from the realm's token endpoint with the given form body. */
  @SneakyThrows
  public String token(String realm, String form) {
    var request =
        new HttpPost(getHostUrl() + "/realms/" + realm + "/protocol/openid-connect/token");
    request.setEntity(new StringEntity(form, ContentType.APPLICATION_FORM_URLENCODED));
    try (var response = sqrl.getHttpClient().execute(request)) {
      var body = EntityUtils.toString(response.getEntity());
      assertThat(response.getStatusLine().getStatusCode()).as(body).isEqualTo(200);
      return MAPPER.readTree(body).get("access_token").asText();
    }
  }
}
