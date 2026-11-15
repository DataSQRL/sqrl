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

import static com.google.common.base.Preconditions.checkArgument;

import io.vertx.core.Vertx;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.OAuth2AuthHandler;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Creates the route handler that authenticates API requests with the configured providers. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AuthHandlerFactory {

  /**
   * Returns a handler that authenticates a request if any of the providers accepts its token.
   *
   * @param vertx the Vert.x instance
   * @param providers the authentication providers, not empty
   * @return the authentication handler
   */
  public static AuthenticationHandler create(Vertx vertx, List<AuthenticationProvider> providers) {
    checkArgument(!providers.isEmpty(), "At least one authentication provider is required");
    if (providers.size() == 1) {
      return create(vertx, providers.get(0));
    }

    var chain = ChainAuthHandler.any();
    providers.forEach(provider -> chain.add(create(vertx, provider)));
    return chain;
  }

  private static AuthenticationHandler create(Vertx vertx, AuthenticationProvider provider) {
    if (provider instanceof JWTAuth jwtAuth) {
      return JWTAuthHandler.create(jwtAuth);
    }
    if (provider instanceof OAuth2Auth oauth2Auth) {
      return OAuth2AuthHandler.create(vertx, oauth2Auth);
    }
    throw new IllegalArgumentException("Unsupported auth provider type: " + provider.getClass());
  }
}
