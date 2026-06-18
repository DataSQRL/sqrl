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
package com.datasqrl.server.modules;

import com.datasqrl.server.GraphQLServerVerticle;
import com.datasqrl.server.McpBridgeVerticle;
import com.datasqrl.server.RestBridgeVerticle;
import com.datasqrl.server.auth.OAuth2AuthFactory;
import com.datasqrl.server.exec.FlinkExecFunctionPlan;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.operation.ApiOperation;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApiDeploymentModule implements ServerModule<VertxServerModuleContext> {

  @Override
  public CompletionStage<Void> configure(VertxServerModuleContext ctx) {
    var deploymentFutures = new ArrayList<Future<String>>();
    for (var modelEntry : ctx.models().entrySet()) {
      var deploymentFuture = deployVersionedModel(ctx, modelEntry.getKey(), modelEntry.getValue());
      deploymentFutures.add(deploymentFuture);
    }

    if (deploymentFutures.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return toCompletionStage(Future.all(deploymentFutures).mapEmpty());
  }

  private Future<String> deployVersionedModel(
      VertxServerModuleContext context, String modelVersion, RootGraphQLModel model) {
    var childOpts = new DeploymentOptions().setInstances(1);
    var hasMcp = model.getOperations().stream().anyMatch(ApiOperation::isMcpEndpoint);
    var hasRest = model.getOperations().stream().anyMatch(ApiOperation::isRestEndpoint);

    return createAuthProviders(context)
        .compose(
            authProviders -> {
              var execFunctionPlan = loadExecFunctionPlan(context.planDir());
              if (execFunctionPlan.isPresent()) {
                log.info("Exec function plan loaded");
              }

              var graphQLVerticle =
                  new GraphQLServerVerticle(
                      context.router(),
                      context.config(),
                      modelVersion,
                      model,
                      authProviders,
                      execFunctionPlan);

              return context
                  .vertx()
                  .deployVerticle(graphQLVerticle, childOpts)
                  .onSuccess(
                      graphQLDeploymentId -> {
                        log.info("GraphQL verticle deployed successfully: {}", graphQLDeploymentId);
                        if (hasMcp) {
                          var mcpBridgeVerticle =
                              new McpBridgeVerticle(
                                  context.router(),
                                  context.config(),
                                  modelVersion,
                                  model,
                                  authProviders,
                                  graphQLVerticle);
                          context
                              .vertx()
                              .deployVerticle(mcpBridgeVerticle, childOpts)
                              .onSuccess(
                                  id ->
                                      log.info("MCP bridge verticle deployed successfully: {}", id))
                              .onFailure(
                                  err -> log.error("Failed to deploy MCP bridge verticle", err));
                        }
                        if (hasRest) {
                          var restBridgeVerticle =
                              new RestBridgeVerticle(
                                  context.router(),
                                  context.config(),
                                  modelVersion,
                                  model,
                                  authProviders,
                                  graphQLVerticle);
                          context
                              .vertx()
                              .deployVerticle(restBridgeVerticle, childOpts)
                              .onSuccess(
                                  id ->
                                      log.info(
                                          "REST bridge verticle deployed successfully: {}", id))
                              .onFailure(
                                  err -> log.error("Failed to deploy REST bridge verticle", err));
                        }
                      })
                  .onFailure(
                      err ->
                          log.error(
                              "Failed to deploy GraphQL verticle, will trigger orderly shutdown",
                              err));
            });
  }

  private Future<List<AuthenticationProvider>> createAuthProviders(
      VertxServerModuleContext context) {
    List<Future<AuthenticationProvider>> providerFutures = new ArrayList<>();

    if (context.config().getOauthConfig() != null
        && context.config().getOauthConfig().getSite() != null) {
      log.info("Configuring OAuth authentication with JWKS");
      providerFutures.add(
          OAuth2AuthFactory.createAuthProvider(context.vertx(), context.config().getOauthConfig())
              .map(provider -> (AuthenticationProvider) provider));
    }

    if (context.config().getJwtAuth() != null) {
      log.info("Configuring JWT authentication");
      providerFutures.add(
          Future.succeededFuture(
              (AuthenticationProvider)
                  JWTAuth.create(context.vertx(), context.config().getJwtAuthOptions())));
    }

    if (providerFutures.isEmpty()) {
      log.info("No authentication configured");
      return Future.succeededFuture(List.of());
    }

    return Future.all(providerFutures)
        .map(
            composite -> {
              List<AuthenticationProvider> providers = new ArrayList<>();
              for (int i = 0; i < composite.size(); i++) {
                providers.add(composite.resultAt(i));
              }
              log.info("Configured {} authentication provider(s)", providers.size());
              return providers;
            });
  }

  private Optional<FlinkExecFunctionPlan> loadExecFunctionPlan(Path planDir) {
    var planFile = planDir.resolve("vertx-exec-functions.ser");

    if (!Files.exists(planFile)) {
      return Optional.empty();
    }

    return Optional.of(FlinkExecFunctionPlan.deserialize(planFile));
  }

  private CompletionStage<Void> toCompletionStage(Future<Void> future) {
    var result = new CompletableFuture<Void>();
    future.onSuccess(result::complete).onFailure(result::completeExceptionally);
    return result;
  }
}
