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
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.auth.oauth2.providers.OpenIDConnectAuth;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for creating OAuth 2.0 authentication providers. Supports JWKS-based key discovery from
 * OAuth providers like Auth0 and Keycloak.
 */
@Slf4j
public class OAuth2AuthFactory {

  /**
   * Creates an authentication provider using OpenID Connect discovery. This enables automatic JWKS
   * key rotation by fetching keys from the provider.
   *
   * @param vertx the Vert.x instance
   * @param config OAuth configuration
   * @return Future containing the AuthenticationProvider, or null if config is invalid
   */
  public static Future<AuthenticationProvider> createAuthProvider(Vertx vertx, OAuthConfig config) {
    if (config == null) {
      return Future.succeededFuture(null);
    }

    var issuer = config.getIssuer();
    if (issuer == null || issuer.isBlank()) {
      log.warn("OAuth config present but no issuer configured");
      return Future.succeededFuture(null);
    }

    var site = issuer.endsWith("/") ? issuer.substring(0, issuer.length() - 1) : issuer;
    log.info("Creating OAuth2 auth provider via OpenID Connect discovery from: {}", site);

    var oauth2Options = new OAuth2Options().setSite(site);

    if (config.getClientId() != null && !config.getClientId().isBlank()) {
      oauth2Options.setClientId(config.getClientId());
    } else {
      oauth2Options.setClientId("datasqrl-mcp");
    }

    return OpenIDConnectAuth.discover(vertx, oauth2Options)
        .map(
            oauth2 -> {
              log.info("OAuth2 auth provider created successfully via OIDC discovery");
              return (AuthenticationProvider) oauth2;
            })
        .recover(
            err -> {
              log.error(
                  "Failed to discover OpenID Connect provider at {}: {}", site, err.getMessage());
              return Future.failedFuture(err);
            });
  }
}
