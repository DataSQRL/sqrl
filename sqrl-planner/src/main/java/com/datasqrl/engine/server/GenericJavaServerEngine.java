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
package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.NO_CAPABILITIES;
import static com.datasqrl.server.config.ServerConfigUtil.mergeConfigs;

import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.ServerConfigConverter;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.EnginePhysicalPlan.ArtifactType;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.dag.plan.ServerStagePlan;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.server.MetadataType;
import com.datasqrl.server.ResolvedMetadata;
import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** A generic java server engine. */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  private final EngineConfig engineConfig;
  private final List<ServerConfigConverter> configConverters;
  private final ErrorCollector errorCollector;

  GenericJavaServerEngine(
      String engineName,
      EngineConfig engineConfig,
      List<ServerConfigConverter> configConverters,
      ErrorCollector errorCollector) {
    super(engineName, EngineType.SERVER, NO_CAPABILITIES);
    this.engineConfig = engineConfig;
    this.configConverters = configConverters;
    this.errorCollector = errorCollector;
  }

  @Override
  @SneakyThrows
  public EnginePhysicalPlan plan(ServerStagePlan serverPlan) {
    validateFunctions(serverPlan.functions());

    var execFnPlan = serverPlan.execFnFactory().getPlan();
    var serverConfig = serverConfig();
    validateAuthProvider(serverPlan, serverConfig);
    var artifacts = new ArrayList<DeploymentArtifact>();
    artifacts.add(new DeploymentArtifact("-config.json", toJson(serverConfig)));

    if (!execFnPlan.functions().isEmpty()) {
      artifacts.add(new DeploymentArtifact("-exec-functions.ser", execFnPlan));
      artifacts.add(new DeploymentArtifact("-exec-functions.json", execFnPlan, ArtifactType.JSON));
    }

    return new ServerPhysicalPlan(
        serverPlan.functions(), serverPlan.mutations(), artifacts, serverConfig);
  }

  private void validateFunctions(List<SqrlTableFunction> functions) {
    var invalidFunctions =
        functions.stream().filter(fn -> fn.getExecutableQuery() == null).toList();

    if (!invalidFunctions.isEmpty()) {
      throw new IllegalStateException(
          "Found function(s) that has not been planned: " + invalidFunctions);
    }
  }

  /**
   * Fails the compile when the script reads a JWT claim, but the server config has no auth
   * provider: no {@code auth} block, no {@code jwtAuth} and no {@code oauthConfig} with a site.
   * Without a provider, the server checks no token, and every claim column fails.
   */
  private void validateAuthProvider(ServerStagePlan serverPlan, ServerConfig serverConfig) {
    if (serverConfig.hasAuthProvider()) {
      return;
    }

    var claimColumns =
        Stream.concat(functionClaimColumns(serverPlan), mutationClaimColumns(serverPlan))
            .sorted()
            .toList();

    errorCollector.checkFatal(
        claimColumns.isEmpty(),
        "The script reads JWT claims (%s), but engines.%s.config has no auth provider. Add an"
            + " auth block with at least one issuer, so the server can check the token. The"
            + " deprecated jwtAuth, or oauthConfig with oauth2Options.site, also gives a provider.",
        String.join(", ", claimColumns),
        getName());
  }

  /**
   * Returns the function parameters that read a claim. Every parameter is a {@link
   * SqrlFunctionParameter}, so the cast fails loudly for another type instead of skipping it.
   */
  private static Stream<String> functionClaimColumns(ServerStagePlan serverPlan) {
    return serverPlan.functions().stream()
        .flatMap(
            function ->
                function.getParameters().stream()
                    .map(SqrlFunctionParameter.class::cast)
                    .flatMap(
                        parameter ->
                            parameter
                                .getMetadata()
                                .filter(GenericJavaServerEngine::isAuthMetadata)
                                .stream()
                                .map(
                                    metadata ->
                                        describeClaimColumn(
                                            function.getFullPath().getDisplay(),
                                            parameter.getName(),
                                            metadata))));
  }

  /** Returns the mutation columns that the server fills from a claim. */
  private static Stream<String> mutationClaimColumns(ServerStagePlan serverPlan) {
    return serverPlan.mutations().stream()
        .flatMap(
            mutation ->
                mutation.getComputedColumns().entrySet().stream()
                    .filter(computedColumn -> isAuthMetadata(computedColumn.getValue()))
                    .map(
                        computedColumn ->
                            describeClaimColumn(
                                mutation.getName().getDisplay(),
                                computedColumn.getKey(),
                                computedColumn.getValue())));
  }

  private static boolean isAuthMetadata(ResolvedMetadata metadata) {
    return metadata.metadataType() == MetadataType.AUTH;
  }

  private static String describeClaimColumn(
      String tableName, String columnName, ResolvedMetadata metadata) {
    return "%s.%s from 'auth.%s'".formatted(tableName, columnName, metadata.name());
  }

  @SneakyThrows
  private ServerConfig serverConfig() {
    return mergeConfigs(readDefaultConfig(), engineConfig.getConfig());
  }

  @SneakyThrows
  private String toJson(ServerConfig serverConfig) {
    return JsonUtils.MAPPER.copy().writer(new PrettyPrinter()).writeValueAsString(serverConfig);
  }

  @SneakyThrows
  ServerConfig readDefaultConfig() {
    try (var input = getClass().getResourceAsStream("/templates/server-config.json")) {
      var serverConfNode = (ObjectNode) JsonUtils.MAPPER.readTree(input);
      configConverters.forEach(converter -> converter.convert(serverConfNode));

      return JsonUtils.MAPPER.treeToValue(serverConfNode, ServerConfig.class).validated();
    }
  }
}
