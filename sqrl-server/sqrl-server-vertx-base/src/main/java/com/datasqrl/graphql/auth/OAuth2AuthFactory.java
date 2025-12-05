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
package com.datasqrl.graphql.auth;

import com.datasqrl.graphql.config.OAuthConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for creating JWT authentication providers with JWKS-based key discovery from OAuth/OIDC
 * providers like Auth0 and Keycloak. Uses JWTAuth instead of OAuth2Auth for proper claim extraction
 * from bearer tokens.
 */
@Slf4j
public class OAuth2AuthFactory {

  private static final String OIDC_DISCOVERY_PATH = "/.well-known/openid-configuration";
  private static final Set<String> JWK_KEYS =
      Set.of("kty", "use", "kid", "alg", "n", "e", "x", "y", "crv");

  /**
   * Creates a JWTAuth provider using JWKS discovered from an OpenID Connect provider. This enables
   * automatic JWKS key discovery while using JWTAuth for proper JWT claim extraction.
   *
   * @param vertx the Vert.x instance
   * @param oauthConfig OAuth configuration containing the OIDC site URL
   * @return Future containing the AuthenticationProvider, or null if config is invalid
   */
  public static Future<AuthenticationProvider> createAuthProvider(
      Vertx vertx, OAuthConfig oauthConfig) {
    if (oauthConfig == null || oauthConfig.getOauth2Options() == null) {
      return Future.succeededFuture(null);
    }

    var oauth2Options = oauthConfig.getOauth2Options();
    var site = oauth2Options.getSite();
    if (site == null || site.isBlank()) {
      log.warn("OAuth config present but no site configured");
      return Future.succeededFuture(null);
    }

    var normalizedSite = site.endsWith("/") ? site.substring(0, site.length() - 1) : site;
    log.info("Creating JWTAuth provider via OIDC JWKS discovery from: {}", normalizedSite);

    var webClient =
        WebClient.create(vertx, new WebClientOptions().setSsl(normalizedSite.startsWith("https")));

    return discoverJwksUri(webClient, normalizedSite)
        .compose(jwksUri -> fetchJwks(webClient, jwksUri))
        .map(
            jwks -> {
              var jwtAuthOptions = new JWTAuthOptions().setJwks(jwks);
              var jwtAuth = JWTAuth.create(vertx, jwtAuthOptions);
              log.info(
                  "JWTAuth provider created successfully with {} keys from OIDC discovery",
                  jwks.size());
              return (AuthenticationProvider) jwtAuth;
            })
        .recover(
            err -> {
              log.error(
                  "Failed to create JWTAuth from OIDC provider at {}: {}",
                  normalizedSite,
                  err.getMessage());
              return Future.failedFuture(err);
            });
  }

  private static Future<String> discoverJwksUri(WebClient webClient, String site) {
    var discoveryUrl = site + OIDC_DISCOVERY_PATH;
    log.debug("Fetching OIDC discovery document from: {}", discoveryUrl);

    return webClient
        .getAbs(discoveryUrl)
        .send()
        .map(
            response -> {
              if (response.statusCode() != 200) {
                throw new RuntimeException(
                    "OIDC discovery failed with status " + response.statusCode());
              }
              var jwksUri = response.bodyAsJsonObject().getString("jwks_uri");
              if (jwksUri == null || jwksUri.isBlank()) {
                throw new IllegalStateException("OIDC discovery document missing jwks_uri");
              }
              log.debug("Discovered JWKS URI: {}", jwksUri);
              return jwksUri;
            });
  }

  private static Future<List<JsonObject>> fetchJwks(WebClient webClient, String jwksUri) {
    log.debug("Fetching JWKS from: {}", jwksUri);

    return webClient
        .getAbs(jwksUri)
        .send()
        .map(
            response -> {
              if (response.statusCode() != 200) {
                throw new RuntimeException(
                    "JWKS fetch failed with status " + response.statusCode());
              }
              var keys = response.bodyAsJsonObject().getJsonArray("keys");
              if (keys == null || keys.isEmpty()) {
                throw new IllegalStateException("JWKS document has no keys");
              }
              log.debug("Fetched {} keys from JWKS", keys.size());
              return convertToJwkOptions(keys);
            });
  }

  private static List<JsonObject> convertToJwkOptions(JsonArray keys) {
    var result = new ArrayList<JsonObject>();
    for (int i = 0; i < keys.size(); i++) {
      var source = keys.getJsonObject(i);
      var jwk = new JsonObject();
      for (var key : JWK_KEYS) {
        if (source.containsKey(key)) {
          jwk.put(key, source.getValue(key));
        }
      }
      result.add(jwk);
    }
    return result;
  }
}
