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

import com.datasqrl.server.config.OAuthConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for creating JWT authentication providers with JWKS-based key discovery from OAuth/OIDC
 * providers like Auth0 and Keycloak, for the deprecated {@code oauthConfig}. Uses JWTAuth instead
 * of OAuth2Auth for proper claim extraction from bearer tokens. The discovery and JWKS fetching is
 * shared with {@link AuthProviderFactory}; unlike {@code auth}, it checks neither {@code iss} nor
 * {@code aud}.
 */
@Slf4j
public class OAuth2AuthFactory {

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

    var discoveryUrl = AuthProviderFactory.discoveryUrl(site);
    log.info("Creating JWTAuth provider via OIDC JWKS discovery from: {}", discoveryUrl);

    var webClient =
        WebClient.create(vertx, new WebClientOptions().setSsl(site.startsWith("https")));

    return AuthProviderFactory.fetchJson(webClient, discoveryUrl)
        .compose(
            discovery ->
                AuthProviderFactory.fetchJwks(webClient, AuthProviderFactory.jwksUri(discovery)))
        .map(
            jwks -> {
              var jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().setJwks(jwks));
              log.info(
                  "JWTAuth provider created successfully with {} keys from OIDC discovery",
                  jwks.size());
              return (AuthenticationProvider) jwtAuth;
            })
        .onComplete(ignored -> webClient.close())
        .recover(
            err -> {
              log.error(
                  "Failed to create JWTAuth from OIDC provider at {}: {}", site, err.getMessage());
              return Future.failedFuture(err);
            });
  }
}
