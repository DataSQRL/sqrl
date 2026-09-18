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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import graphql.GraphQL;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class GraphQLServerVerticleTest {

  @Test
  void givenGraphQLEndpointDisabled_whenStarted_thenRetainsEngineWithoutRegisteringRoutes(
      Vertx vertx, VertxTestContext testContext) {
    var router = mock(Router.class);
    var model =
        RootGraphQLModel.builder()
            .schema(StringSchema.builder().schema("type Query { greeting: String }").build())
            .build();
    var verticle = new TestGraphQLServerVerticle(router, model);

    vertx
        .deployVerticle(verticle)
        .onComplete(
            testContext.succeeding(
                ignored -> {
                  testContext.verify(
                      () -> {
                        assertThat(verticle.getGraphQLEngine()).isNotNull();
                        verifyNoInteractions(router);
                      });
                  testContext.completeNow();
                }));
  }

  private static class TestGraphQLServerVerticle extends GraphQLServerVerticle {

    TestGraphQLServerVerticle(Router router, RootGraphQLModel model) {
      super(router, graphQLEndpointDisabledConfig(), "v1", model, List.of(), Optional.empty());
    }

    private static ServerConfig graphQLEndpointDisabledConfig() {
      var config = new ServerConfig();
      config.setPublicGraphQLEndpointEnabled(false);
      return config;
    }

    @Override
    protected GraphQL createGraphQLEngine(SubscriptionConfigurationImpl subscriptionConfig) {
      return mock(GraphQL.class);
    }
  }
}
