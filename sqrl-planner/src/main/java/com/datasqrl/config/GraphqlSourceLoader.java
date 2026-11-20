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
package com.datasqrl.config;

import static com.datasqrl.server.ApiSources.DEFAULT_API_VERSION;

import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.server.ApiSource;
import com.datasqrl.server.ApiSources;
import com.datasqrl.server.GraphqlSchemaHandler;
import com.datasqrl.server.ScriptFiles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
@Lazy
@RequiredArgsConstructor
public class GraphqlSourceLoader {

  private final ScriptFiles scriptFiles;
  private final WorkspacePaths workspacePaths;
  private final GraphqlSchemaHandler graphqlSchemaHandler;
  private final PackageJson config;
  private final ExecutionGoal executionGoal;
  private final ErrorCollector errors;

  public record LoadResult(List<ApiSources> apiVersions, Optional<String> inferredSchema) {}

  /**
   * Loads the GraphQL schema and operation sources for every configured API version.
   *
   * <p>Uses an explicitly configured schema when present. Otherwise, generates a schema from the
   * server plan when no GraphQL schema is configured, a versioned API omits its schema, or tests
   * request the inferred schema. Inferred schemas retain the configured API version and operations.
   *
   * @param serverPlan the physical plan from which an inferred schema is generated
   * @return the sources for each API version and, when generated, the inferred schema
   */
  public LoadResult load(ServerPhysicalPlan serverPlan) {
    if (!scriptFiles.getApiConfigs().isEmpty()) {
      return loadVersionedApis(serverPlan);
    }

    var apiVersions =
        scriptFiles
            .getGraphql()
            .map(
                schema ->
                    List.of(
                        createApiSources(
                            DEFAULT_API_VERSION,
                            "script.graphql",
                            schema,
                            "script.operations",
                            scriptFiles.getOperations())))
            .orElse(List.of());

    if (!shouldUseInferredSchema(apiVersions)) {
      apiVersions.forEach(
          apiVersion -> graphqlSchemaHandler.validateSchema(apiVersion, serverPlan));
      return new LoadResult(apiVersions, Optional.empty());
    }

    var inferredSchema = graphqlSchemaHandler.inferGraphQLSchema(serverPlan);
    apiVersions =
        List.of(
            new ApiSources(
                inferredSchema,
                apiVersions.isEmpty()
                    ? resolveOperations("script.operations", scriptFiles.getOperations())
                    : apiVersions.stream().flatMap(a -> a.operations().stream()).toList()));

    return new LoadResult(apiVersions, Optional.of(inferredSchema));
  }

  private LoadResult loadVersionedApis(ServerPhysicalPlan serverPlan) {
    var apiConfigs = scriptFiles.getApiConfigs();
    var inferForTests =
        executionGoal == ExecutionGoal.TEST && config.getTestConfig().useInferredSchema();
    var inferForMissingSchema =
        apiConfigs.stream().anyMatch(apiConfig -> apiConfig.getSchema().isEmpty());

    if (!inferForTests && !inferForMissingSchema) {
      var apiVersions =
          apiConfigs.stream()
              .map(
                  apiConf ->
                      createApiSources(
                          apiConf.getVersion(),
                          apiKey(apiConf, "schema"),
                          apiConf.getSchema().orElseThrow(),
                          apiKey(apiConf, "operations"),
                          apiConf.getOperations()))
              .toList();

      apiVersions.forEach(
          apiVersion -> graphqlSchemaHandler.validateSchema(apiVersion, serverPlan));

      return new LoadResult(apiVersions, Optional.empty());
    }

    var inferredSchema = graphqlSchemaHandler.inferGraphQLSchema(serverPlan);
    var apiVersions =
        apiConfigs.stream()
            .map(apiConfig -> createVersionedApiSources(apiConfig, inferredSchema, inferForTests))
            .toList();

    if (!inferForTests) {
      apiVersions.stream()
          .filter(apiVersion -> apiVersion.schema().getPath().isPresent())
          .forEach(apiVersion -> graphqlSchemaHandler.validateSchema(apiVersion, serverPlan));
    }
    return new LoadResult(apiVersions, Optional.of(inferredSchema));
  }

  private ApiSources createVersionedApiSources(
      PackageJson.ScriptApiConfig apiConfig, String inferredSchema, boolean forceInferredSchema) {
    var operations = resolveOperations(apiKey(apiConfig, "operations"), apiConfig.getOperations());
    var configuredSchema =
        apiConfig.getSchema().map(schema -> resolvePath(apiKey(apiConfig, "schema"), schema));
    if (forceInferredSchema || apiConfig.getSchema().isEmpty()) {
      return new ApiSources(apiConfig.getVersion(), new ApiSource(inferredSchema), operations);
    }
    return new ApiSources(apiConfig.getVersion(), configuredSchema.orElseThrow(), operations);
  }

  private boolean shouldUseInferredSchema(List<ApiSources> apiVersions) {
    return apiVersions.isEmpty()
        || (executionGoal == ExecutionGoal.TEST && config.getTestConfig().useInferredSchema());
  }

  private ApiSources createApiSources(
      String version,
      String schemaKey,
      String schema,
      String operationsKey,
      List<String> operations) {

    var schemaSrc = resolvePath(schemaKey, schema);
    var opSrc = resolveOperations(operationsKey, operations);

    return new ApiSources(version, schemaSrc, opSrc);
  }

  /** Returns the full key of a path of an API version, for error messages. */
  private static String apiKey(PackageJson.ScriptApiConfig apiConfig, String key) {
    return "script.api.%s.%s".formatted(apiConfig.getVersion(), key);
  }

  private List<ApiSource> resolveOperations(String key, List<String> operations) {
    return operations.stream().map(file -> resolvePath(key, file)).toList();
  }

  @SneakyThrows
  private ApiSource resolvePath(String key, String file) {
    var path = workspacePaths.resolveConfiguredPath(key, file, errors);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException("Failed to find file: " + file);
    }

    return new ApiSource(Path.of(file), Files.readString(path));
  }
}
