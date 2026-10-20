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

import static com.datasqrl.server.auth.ProtectedResourceUrlsTest.configWithResource;
import static com.datasqrl.server.auth.ProtectedResourceUrlsTest.request;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;

class OAuthFailureHandlerTest {

  @Test
  void givenResourceBehindHttpProxy_whenAuthFails_thenMetadataUrlUsesResourceOrigin() {
    var config = configWithResource("https://api.example.com/v1/mcp");
    config.getOauthConfig().setOauth2Options(new OAuth2Options().setSite("https://auth.example"));
    var request = request("http", "api.example.com", "http");
    var response = mock(HttpServerResponse.class, RETURNS_SELF);
    var ctx = mock(RoutingContext.class);
    when(ctx.statusCode()).thenReturn(401);
    when(ctx.request()).thenReturn(request);
    when(ctx.response()).thenReturn(response);

    new OAuthFailureHandler(config).handle(ctx);

    verify(response)
        .putHeader(
            "WWW-Authenticate",
            "Bearer resource_metadata=\"https://api.example.com/.well-known/oauth-protected-resource\"");
  }
}
