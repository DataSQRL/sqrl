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
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates one {@link JWTAuth} per trusted issuer of the {@code auth} config, each bound to that
 * issuer's own keys, audiences, leeway and algorithms. Keys are loaded once at startup; any failure
 * fails the returned future so the server does not start.
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
        authConfig.getIssuers().stream()
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

  List<JsonObject> keysForAlgorithms(Issuer issuer, List<JsonObject> keys) {
    var usable =
        keys.stream()
            .filter(key -> !"enc".equals(key.getString("use")))
            .flatMap(key -> withAllowedAlgorithms(key, issuer.getAlgorithms()))
            .toList();
    checkState(
        !usable.isEmpty(),
        "no signing key of issuer '%s' is usable with algorithms %s",
        issuer.getIssuer(),
        issuer.getAlgorithms());
    return usable;
  }

  private Future<AuthenticationProvider> createAuthProvider(
      Vertx vertx, WebClient webClient, Issuer issuer) {
    return loadKeys(webClient, issuer)
        .map(
            keys -> {
              var jwks = keysForAlgorithms(issuer, keys);
              var jwtOptions =
                  new JWTOptions()
                      .setIssuer(issuer.getIssuer())
                      .setAudience(issuer.getAudiences())
                      .setLeeway(issuer.getLeeway());
              log.info(
                  "Trusting issuer {} for audiences {} with {} key(s)",
                  issuer.getIssuer(),
                  issuer.getAudiences(),
                  jwks.size());
              return (AuthenticationProvider)
                  JWTAuth.create(
                      vertx, new JWTAuthOptions().setJwks(jwks).setJWTOptions(jwtOptions));
            })
        .recover(
            err ->
                Future.failedFuture(
                    new IllegalStateException(
                        "Cannot configure trusted issuer '%s': %s"
                            .formatted(issuer.getIssuer(), err.getMessage()),
                        err)));
  }

  private Future<List<JsonObject>> loadKeys(WebClient webClient, Issuer issuer) {
    if (issuer.getKeys() != null) {
      return Future.succeededFuture(
          issuer.getKeys().stream().map(AuthProviderFactory::publicJwk).toList());
    }
    if (issuer.getJwksUri() != null) {
      return fetchJwks(webClient, issuer.getJwksUri());
    }

    return fetchJson(webClient, discoveryUrl(issuer.getIssuer()))
        .compose(
            discovery -> {
              checkState(
                  issuer.getIssuer().equals(discovery.getValue("issuer")),
                  "OIDC discovery document names issuer '%s'",
                  discovery.getValue("issuer"));
              var jwksUri = discovery.getValue("jwks_uri");
              checkState(
                  jwksUri instanceof String uri && !uri.isBlank(),
                  "OIDC discovery document has no jwks_uri");
              return fetchJwks(webClient, (String) jwksUri);
            });
  }

  private Future<List<JsonObject>> fetchJwks(WebClient webClient, String jwksUri) {
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
                  .map(key -> publicJwk((JsonObject) key))
                  .toList();
            });
  }

  private Future<JsonObject> fetchJson(WebClient webClient, String url) {
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

  private JsonObject publicJwk(JsonObject source) {
    var jwk = new JsonObject();
    JWK_KEYS.stream()
        .filter(source::containsKey)
        .forEach(key -> jwk.put(key, source.getValue(key)));
    return jwk;
  }

  private Stream<JsonObject> withAllowedAlgorithms(JsonObject key, List<String> algorithms) {
    var kty = key.getString("kty");
    var alg = key.getString("alg");
    if (alg != null) {
      return algorithms.contains(alg) && isCompatible(kty, alg) ? Stream.of(key) : Stream.empty();
    }
    return algorithms.stream()
        .filter(candidate -> isCompatible(kty, candidate))
        .map(candidate -> key.copy().put("alg", candidate));
  }

  private boolean isCompatible(String kty, String alg) {
    return switch (kty == null ? "" : kty) {
      case "RSA" -> alg.startsWith("RS") || alg.startsWith("PS");
      case "EC" -> alg.startsWith("ES");
      case "oct" -> alg.startsWith("HS");
      case "OKP" -> alg.equals("EdDSA");
      default -> false;
    };
  }
}
