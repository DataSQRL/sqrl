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
import com.datasqrl.loaders.resolver.ResourceResolver;
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
  private final ResourceResolver resolver;
  private final GraphqlSchemaHandler graphqlSchemaHandler;
  private final PackageJson config;
  private final ExecutionGoal executionGoal;

  public record LoadResult(List<ApiSources> apiVersions, Optional<String> inferredSchema) {}

  public LoadResult load(ServerPhysicalPlan serverPlan) {
    List<ApiSources> apiVersions;

    if (!scriptFiles.getApiConfigs().isEmpty()) {
      apiVersions =
          scriptFiles.getApiConfigs().stream()
              .map(
                  apiConf ->
                      createApiSources(
                          apiConf.getVersion(),
                          apiConf.getSchema(),
                          apiConf.getOperations(),
                          resolver))
              .toList();

    } else if (scriptFiles.getGraphql().isEmpty()) {
      apiVersions = List.of();

    } else {
      var sources =
          createApiSources(
              DEFAULT_API_VERSION,
              scriptFiles.getGraphql().get(),
              scriptFiles.getOperations(),
              resolver);
      apiVersions = List.of(sources);
    }

    if (!shouldUseInferredSchema(apiVersions)) {
      apiVersions.forEach(
          apiVersion -> graphqlSchemaHandler.validateSchema(apiVersion, serverPlan));
      return new LoadResult(apiVersions, Optional.empty());
    }

    List<ApiSource> operations;
    if (apiVersions.isEmpty()) {
      operations =
          scriptFiles.getOperations().stream().map(file -> resolvePath(file, resolver)).toList();
    } else {
      operations = apiVersions.stream().flatMap(a -> a.operations().stream()).toList();
    }

    var inferredSchema = graphqlSchemaHandler.inferGraphQLSchema(serverPlan);
    return new LoadResult(
        List.of(new ApiSources(inferredSchema, operations)), Optional.of(inferredSchema));
  }

  private boolean shouldUseInferredSchema(List<ApiSources> apiVersions) {
    return apiVersions.isEmpty()
        || (executionGoal == ExecutionGoal.TEST && config.getTestConfig().useInferredSchema());
  }

  private static ApiSources createApiSources(
      String version, String schema, List<String> operations, ResourceResolver resolver) {

    var schemaSrc = resolvePath(schema, resolver);
    var opSrc = operations.stream().map(file -> resolvePath(file, resolver)).toList();

    return new ApiSources(version, schemaSrc, opSrc);
  }

  @SneakyThrows
  private static ApiSource resolvePath(String file, ResourceResolver resolver) {
    var relativePath = Path.of(file);
    var absolutePath =
        resolver
            .resolveFile(relativePath)
            .orElseThrow(() -> new IllegalArgumentException("Failed to find file: " + file));

    return new ApiSource(relativePath, Files.readString(absolutePath));
  }
}
