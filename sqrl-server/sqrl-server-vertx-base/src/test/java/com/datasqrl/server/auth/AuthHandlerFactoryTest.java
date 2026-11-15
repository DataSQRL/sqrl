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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.vertx.core.Vertx;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuthHandlerFactoryTest {

  private Vertx vertx;
  private JWTAuth jwtAuth;

  @BeforeEach
  void setUp() {
    vertx = Vertx.vertx();
    jwtAuth =
        JWTAuth.create(
            vertx,
            new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions().setAlgorithm("HS256").setBuffer("dGVzdA==")));
  }

  @AfterEach
  void tearDown() {
    vertx.close();
  }

  @Test
  void givenSingleJwtProvider_whenCreating_thenReturnsJwtHandler() {
    var handler = AuthHandlerFactory.create(vertx, List.of(jwtAuth));

    assertThat(handler).isInstanceOf(JWTAuthHandler.class);
  }

  @Test
  void givenSeveralProviders_whenCreating_thenReturnsChainHandler() {
    var handler = AuthHandlerFactory.create(vertx, List.of(jwtAuth, jwtAuth));

    assertThat(handler).isInstanceOf(ChainAuthHandler.class);
  }

  @Test
  void givenUnsupportedProvider_whenCreating_thenThrows() {
    var provider = mock(AuthenticationProvider.class);

    assertThatThrownBy(() -> AuthHandlerFactory.create(vertx, List.of(provider)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Unsupported auth provider type");
  }

  @Test
  void givenNoProviders_whenCreating_thenThrows() {
    assertThatThrownBy(() -> AuthHandlerFactory.create(vertx, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
