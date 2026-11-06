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

import static com.google.common.base.Preconditions.checkState;

import com.datasqrl.server.config.AuthConfig;
import com.datasqrl.server.config.AuthConfig.Issuer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates one {@link JWTAuth} per trusted issuer of the {@code auth} config, each bound to that
 * issuer's own keys, audiences and leeway. Keys are loaded once at startup; any failure fails the
 * returned future so the server does not start.
 */
@Slf4j
@UtilityClass
public class AuthProviderFactory {

  private static final String OIDC_DISCOVERY_PATH = "/.well-known/openid-configuration";
  private static final Set<String> JWK_KEYS =
      Set.of("kty", "use", "kid", "alg", "n", "e", "x", "y", "crv", "k");
  private static final long FETCH_TIMEOUT_MS = 10_000;

  public Future<List<AuthenticationProvider>> createAuthProviders(
      Vertx vertx, AuthConfig authConfig) {
    var webClient = WebClient.create(vertx);
    var providers =
        authConfig.issuers().stream()
            .map(issuer -> createAuthProvider(vertx, webClient, issuer))
            .toList();
    return Future.all(providers)
        .map(composite -> composite.<AuthenticationProvider>list())
        .onComplete(ignored -> webClient.close());
  }

  String discoveryUrl(String issuer) {
    var base = issuer.endsWith("/") ? issuer.substring(0, issuer.length() - 1) : issuer;
    return base + OIDC_DISCOVERY_PATH;
  }

  private Future<AuthenticationProvider> createAuthProvider(
      Vertx vertx, WebClient webClient, Issuer issuer) {
    return loadKeys(webClient, issuer)
        .map(issuer::keysForAlgorithms)
        .compose(
            jwks -> {
              var jwtOptions =
                  new JWTOptions()
                      .setIssuer(issuer.issuer())
                      .setAudience(issuer.audiences())
                      .setLeeway(issuer.leeway());
              log.info(
                  "Trusting issuer {} for audiences {} with {} key(s)",
                  issuer.issuer(),
                  issuer.audiences(),
                  jwks.size());
              return createJwtAuth(
                  vertx, new JWTAuthOptions().setJwks(jwks).setJWTOptions(jwtOptions));
            })
        .recover(
            err ->
                Future.failedFuture(
                    new IllegalStateException(
                        "Cannot configure trusted issuer '%s': %s"
                            .formatted(issuer.issuer(), err.getMessage()),
                        err)));
  }

  /**
   * Creates a {@link JWTAuth} and makes sure it verifies signatures. Vert.x skips every key it
   * cannot load with only a warning, and a {@link JWTAuth} left without keys accepts unsigned
   * tokens. So the new provider is probed with an unsigned token that passes its issuer, audience
   * and expiry checks, and the returned future fails if the provider accepts it.
   */
  public Future<AuthenticationProvider> createJwtAuth(Vertx vertx, JWTAuthOptions options) {
    var provider = JWTAuth.create(vertx, options);
    var jwtOptions = options.getJWTOptions();
    var claims = new JsonObject().put("exp", Instant.now().plusSeconds(60).getEpochSecond());
    if (jwtOptions.getIssuer() != null) {
      claims.put("iss", jwtOptions.getIssuer());
    }
    if (jwtOptions.getAudience() != null) {
      claims.put("aud", new JsonArray(jwtOptions.getAudience()));
    }
    var unsignedToken = base64Url(new JsonObject().put("alg", "none")) + "." + base64Url(claims);
    return provider
        .authenticate(new TokenCredentials(unsignedToken))
        .transform(
            probe ->
                probe.succeeded()
                    ? Future.failedFuture(
                        new IllegalStateException(
                            "no key can verify signatures, so unsigned tokens would be accepted"))
                    : Future.succeededFuture(provider));
  }

  private String base64Url(JsonObject json) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(json.encode().getBytes(StandardCharsets.UTF_8));
  }

  private Future<List<JsonObject>> loadKeys(WebClient webClient, Issuer issuer) {
    if (issuer.keys() != null) {
      return Future.succeededFuture(
          issuer.keys().stream().map(AuthProviderFactory::verificationJwk).toList());
    }
    if (issuer.jwksUri() != null) {
      return fetchJwks(webClient, issuer.jwksUri());
    }

    return fetchJson(webClient, discoveryUrl(issuer.issuer()))
        .compose(
            discovery -> {
              checkState(
                  issuer.issuer().equals(discovery.getValue("issuer")),
                  "OIDC discovery document names issuer '%s'",
                  discovery.getValue("issuer"));
              return fetchJwks(webClient, jwksUri(discovery));
            });
  }

  /** Returns the {@code jwks_uri} of an OIDC discovery document. */
  String jwksUri(JsonObject discovery) {
    var jwksUri = discovery.getValue("jwks_uri");
    checkState(
        jwksUri instanceof String uri && !uri.isBlank(), "OIDC discovery document has no jwks_uri");
    return (String) jwksUri;
  }

  /** Fetches a JWK Set and returns its keys, reduced to the fields needed to verify signatures. */
  Future<List<JsonObject>> fetchJwks(WebClient webClient, String jwksUri) {
    return fetchJson(webClient, jwksUri)
        .map(
            jwks -> {
              checkState(
                  jwks.getValue("keys") instanceof JsonArray keys
                      && !keys.isEmpty()
                      && keys.stream().allMatch(JsonObject.class::isInstance),
                  "JWKS at %s has no keys",
                  jwksUri);
              return jwks.getJsonArray("keys").stream()
                  .map(key -> verificationJwk((JsonObject) key))
                  .toList();
            });
  }

  /** GETs the URL and returns its body as a JSON object, failing on any other status or body. */
  Future<JsonObject> fetchJson(WebClient webClient, String url) {
    return webClient
        .getAbs(url)
        .timeout(FETCH_TIMEOUT_MS)
        .send()
        .map(
            response -> {
              checkState(
                  response.statusCode() == 200,
                  "GET %s returned status %s",
                  url,
                  response.statusCode());
              try {
                var body = response.bodyAsJsonObject();
                checkState(body != null, "GET %s returned an empty body", url);
                return body;
              } catch (DecodeException e) {
                throw new IllegalStateException(
                    "GET %s did not return a JSON object".formatted(url));
              }
            });
  }

  /**
   * Keeps the JWK fields needed to verify signatures and drops the rest, such as private RSA or EC
   * parameters. For an {@code oct} key that includes {@code k}, the shared secret.
   */
  private JsonObject verificationJwk(JsonObject source) {
    var jwk = new JsonObject();
    JWK_KEYS.stream()
        .filter(source::containsKey)
        .forEach(key -> jwk.put(key, source.getValue(key)));
    return jwk;
  }
}
