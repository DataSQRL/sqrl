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
package com.datasqrl.server.auth;

import static com.datasqrl.server.auth.TestJwtKeys.claims;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.server.config.AuthConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.TokenCredentials;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuthProviderFactoryTest {

  private static final String AUDIENCE = "https://api.example.com/";

  private Vertx vertx;
  private TestJwtKeys rsa;
  private HttpServer idp;
  private final Map<String, Object> responses = new ConcurrentHashMap<>();

  @BeforeEach
  void setup() {
    vertx = Vertx.vertx();
    rsa = TestJwtKeys.rsa(vertx);
    idp =
        await(
            vertx
                .createHttpServer()
                .requestHandler(
                    request -> {
                      var body = responses.get(request.path());
                      if (body == null) {
                        request.response().setStatusCode(404).end();
                      } else {
                        request.response().end(body.toString());
                      }
                    })
                .listen(0));
  }

  @AfterEach
  void teardown() {
    await(vertx.close());
  }

  @Test
  void givenIssuerWithTrailingSlash_whenDiscovered_thenKeysFromJwksUriVerifyItsTokens() {
    var issuer = idpUrl("/");
    serveDiscovery("/.well-known/openid-configuration", issuer, idpUrl("/jwks"));
    serveJwks("/jwks");

    var provider = singleProvider(issuer(issuer));

    assertThat(authenticates(provider, rsa.token(claims(issuer, AUDIENCE)))).isTrue();
    assertThat(authenticates(provider, rsa.token(claims(idpUrl("/other/"), AUDIENCE)))).isFalse();
  }

  @Test
  void givenIssuerWithPath_whenDiscovered_thenDiscoveryDocumentIsBelowThatPath() {
    var issuer = idpUrl("/realms/demo");
    serveDiscovery("/realms/demo/.well-known/openid-configuration", issuer, idpUrl("/certs"));
    serveJwks("/certs");

    var provider = singleProvider(issuer(issuer));

    assertThat(authenticates(provider, rsa.token(claims(issuer, AUDIENCE)))).isTrue();
  }

  @Test
  void givenJwksUri_whenCreated_thenKeysAreFetchedWithoutDiscovery() {
    var issuer = "https://issuer.example.com/";
    serveJwks("/jwks");
    var entry = issuer(issuer);
    entry.put("jwksUri", idpUrl("/jwks"));

    var provider = singleProvider(entry);

    assertThat(authenticates(provider, rsa.token(claims(issuer, AUDIENCE)))).isTrue();
  }

  @Test
  void givenUnreachableIdentityProvider_whenCreated_thenFailsFast() {
    var issuer = "http://localhost:1/";

    assertThatThrownBy(() -> singleProvider(issuer(issuer)))
        .hasMessageContaining("Cannot configure trusted issuer 'http://localhost:1/'");
  }

  @Test
  void givenMissingDiscoveryDocument_whenCreated_thenFailsFast() {
    var issuer = idpUrl("/");

    assertThatThrownBy(() -> singleProvider(issuer(issuer)))
        .hasMessageContaining("/.well-known/openid-configuration returned status 404");
  }

  @Test
  void givenDiscoveryDocumentForOtherIssuer_whenCreated_thenFailsFast() {
    var issuer = idpUrl("/");
    serveDiscovery(
        "/.well-known/openid-configuration", "https://other.example.com/", idpUrl("/jwks"));
    serveJwks("/jwks");

    assertThatThrownBy(() -> singleProvider(issuer(issuer)))
        .hasMessageContaining("OIDC discovery document names issuer 'https://other.example.com/'");
  }

  @Test
  void givenDiscoveryDocumentWithoutJwksUri_whenCreated_thenFailsFast() {
    var issuer = idpUrl("/");
    responses.put("/.well-known/openid-configuration", new JsonObject().put("issuer", issuer));

    assertThatThrownBy(() -> singleProvider(issuer(issuer)))
        .hasMessageContaining("OIDC discovery document has no jwks_uri");
  }

  @Test
  void givenMalformedJwks_whenCreated_thenFailsFast() {
    var issuer = idpUrl("/");
    serveDiscovery("/.well-known/openid-configuration", issuer, idpUrl("/jwks"));
    responses.put("/jwks", "not json");

    assertThatThrownBy(() -> singleProvider(issuer(issuer)))
        .hasMessageContaining("/jwks did not return a JSON object");
  }

  @Test
  void givenJwksWithoutKeys_whenCreated_thenFailsFast() {
    var issuer = idpUrl("/");
    serveDiscovery("/.well-known/openid-configuration", issuer, idpUrl("/jwks"));
    responses.put("/jwks", new JsonObject().put("keys", new JsonArray()));

    assertThatThrownBy(() -> singleProvider(issuer(issuer)))
        .hasMessageContaining("/jwks has no keys");
  }

  @Test
  void givenKeysUnusableWithAllowedAlgorithms_whenCreated_thenFailsFast() {
    var hmac = TestJwtKeys.hmac(vertx, "a-secret-that-is-long-enough-for-hs256");
    var entry = issuer("https://issuer.example.com/");
    entry.put("keys", List.of(hmac.publicJwk().getMap()));

    assertThatThrownBy(() -> singleProvider(entry))
        .hasMessageContaining(
            "no signing key of issuer 'https://issuer.example.com/' is usable with algorithms"
                + " [RS256]");
  }

  @Test
  void givenIssuerUrls_whenBuildingDiscoveryUrl_thenTrailingSlashIsNotDoubled() {
    assertThat(AuthProviderFactory.discoveryUrl("https://tenant.auth0.com/"))
        .isEqualTo("https://tenant.auth0.com/.well-known/openid-configuration");
    assertThat(AuthProviderFactory.discoveryUrl("https://idp.example.com/realms/demo"))
        .isEqualTo("https://idp.example.com/realms/demo/.well-known/openid-configuration");
  }

  private Map<String, Object> issuer(String issuer) {
    var entry = new HashMap<String, Object>();
    entry.put("issuer", issuer);
    entry.put("audiences", List.of(AUDIENCE));
    return entry;
  }

  private AuthenticationProvider singleProvider(Map<String, Object> issuer) {
    var authConfig = AuthConfig.parse(new JsonObject(Map.of("issuers", List.of(issuer))));
    var providers = awaitOrThrow(AuthProviderFactory.createAuthProviders(vertx, authConfig));
    assertThat(providers).hasSize(1);
    return providers.get(0);
  }

  private boolean authenticates(AuthenticationProvider provider, String token) {
    return awaitCompletion(provider.authenticate(new TokenCredentials(token)));
  }

  private void serveDiscovery(String path, String issuer, String jwksUri) {
    responses.put(path, new JsonObject().put("issuer", issuer).put("jwks_uri", jwksUri));
  }

  private void serveJwks(String path) {
    responses.put(path, new JsonObject().put("keys", new JsonArray().add(rsa.publicJwk())));
  }

  private String idpUrl(String path) {
    return "http://localhost:" + idp.actualPort() + path;
  }

  @SneakyThrows
  private static boolean awaitCompletion(Future<?> future) {
    try {
      future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      return true;
    } catch (ExecutionException e) {
      return false;
    }
  }

  @SneakyThrows
  private static <T> T awaitOrThrow(Future<T> future) {
    try {
      return future.toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @SneakyThrows
  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }
}
