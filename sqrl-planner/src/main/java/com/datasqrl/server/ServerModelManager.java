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

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.converter.GraphQLSchemaConverter;
import com.datasqrl.server.converter.GraphQLSchemaConverterConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import com.datasqrl.server.openapi.OpenApiService;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.ApiProtocol;
import com.datasqrl.server.operation.McpApp;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.openapitools.openapidiff.core.OpenApiCompare;
import org.openapitools.openapidiff.core.model.ChangedOpenApi;
import org.openapitools.openapidiff.core.output.ConsoleRender;
import org.springframework.stereotype.Component;

/** Generates the model for the server */
@Component
@AllArgsConstructor
public class ServerModelManager {

  private final PackageJson packageConfig;
  private final WorkspacePaths workspacePaths;
  private final GraphQLSchemaConverter converter;
  private final ErrorCollector errors;

  /**
   * Generates the {@link RootGraphQLModel} from the server plan and defined operations
   *
   * @param api Contains the GraphQL schema and any defined operations
   * @param serverPlan The physical plan for the server with all function definitions
   * @return
   */
  public RootGraphQLModel generateGraphQLModel(ApiSources api, ServerPhysicalPlan serverPlan) {
    var graphqlModelGenerator =
        new GraphqlModelGenerator(serverPlan.getFunctions(), serverPlan.getMutations(), errors);
    graphqlModelGenerator.walkAPISource(api.schema());
    serverPlan.getPagedRowTimeTables().addAll(graphqlModelGenerator.getPagedRowTimeTables());
    serverPlan.getServedFunctions().addAll(graphqlModelGenerator.getSeenTableFunctions());
    var schema = StringSchema.builder().schema(api.schema().getDefinition()).build();
    var graphSchema = converter.getSchema(schema.getSchema());
    var apiConfig = packageConfig.getCompilerConfig().getApiConfig();
    var converterConfig =
        GraphQLSchemaConverterConfig.builder()
            .addPrefix(apiConfig.isAddOperationsPrefix())
            .maxDepth(apiConfig.getMaxResultDepth())
            .protocols(apiConfig.getProtocols())
            .build();
    var localErrors = errors.withScript(api.schema().getPath(), api.schema().getDefinition());
    var definedOperations = new ArrayList<ApiOperation>();
    // First, convert all explicitly defined operations, preserving the original order
    for (var operationFile : api.operations()) {
      localErrors = errors.withScript(operationFile.getPath(), operationFile.getDefinition());
      try {
        definedOperations.addAll(
            converter.convertOperations(
                operationFile.getDefinition(), converterConfig, graphSchema));
      } catch (Throwable e) {
        throw localErrors.handle(e);
      }
    }
    // Second, we add the automatically generated operations
    if (apiConfig.generateOperations()) {
      try {
        definedOperations.addAll(converter.convertSchema(converterConfig, graphSchema));
      } catch (Throwable e) {
        throw localErrors.handle(e);
      }
    }
    // Third, distincting preserves only the first operation by id
    var dedupedOperations = definedOperations.stream().distinct().toList();

    // Fourth, link the MCP tools to the MCP apps
    var mcpEnabled = apiConfig.getProtocols().contains(ApiProtocol.MCP);
    var mcpApps = loadMcpApps(api.version(), mcpEnabled);
    final List<ApiOperation> operations;
    if (mcpApps.isPresent()) {
      operations = McpAppLinker.link(dedupedOperations, mcpApps.get(), errors);
    } else if (mcpEnabled) {
      operations = McpAppLinker.withoutApps(dedupedOperations, api.version(), errors);
    } else {
      operations = McpAppLinker.unlinked(dedupedOperations);
    }

    return RootGraphQLModel.builder()
        .queries(graphqlModelGenerator.getQueryCoords())
        .mutations(graphqlModelGenerator.getMutations())
        .subscriptions(graphqlModelGenerator.getSubscriptions())
        .operations(operations)
        .schema(schema)
        .mcpApps(mcpApps.orElse(null))
        .build();
  }

  /**
   * Loads the MCP apps of the API version from its apps directory in the build directory, where the
   * packager copies the {@code .html} files with the other source files, or returns empty if it has
   * no MCP Apps support: no apps directory is configured, or MCP is not one of the API protocols.
   */
  private Optional<List<McpApp>> loadMcpApps(String apiVersion, boolean mcpEnabled) {
    var directory = findApiConfig(apiVersion).flatMap(PackageJson.ScriptApiConfig::getMcpApps);
    if (directory.isEmpty()) {
      return Optional.empty();
    }
    if (!mcpEnabled) {
      errors.warn(
          "The MCP apps in '%s' are not served because 'compiler.api.protocols' does not include"
              + " MCP",
          directory.get());
      return Optional.empty();
    }
    var buildDirectory = workspacePaths.buildDir().resolve(directory.get());
    // The build directory only has the apps directory once it has an app, since the packager
    // copies the source files into the build directory but no empty directories
    if (!Files.isDirectory(buildDirectory)
        && Files.isDirectory(workspacePaths.projectRoot().resolve(directory.get()))) {
      return Optional.of(List.of());
    }
    return Optional.of(McpAppLinker.loadApps(buildDirectory, directory.get(), errors));
  }

  private Optional<PackageJson.ScriptApiConfig> findApiConfig(String apiVersion) {
    return packageConfig.getScriptConfig().getScriptApiConfigs().stream()
        .filter(apiConfig -> apiConfig.getVersion().equals(apiVersion))
        .findFirst();
  }

  public String generateOpenApiJson(
      ApiSources api, RootGraphQLModel model, ServerConfig serverConfig) {
    var openApiService =
        new OpenApiService(
            serverConfig.getOpenApiConfig(),
            model,
            api.version(),
            serverConfig.getServletConfig().getRestEndpoint(api.version()));

    var generatedOpenApi = openApiService.generateOpenApiJson();
    validateOpenApiJsonIfNeeded(api.version(), generatedOpenApi);

    return generatedOpenApi;
  }

  private void validateOpenApiJsonIfNeeded(String apiVersion, String generatedOpenApi) {
    var configuredFilenameOpt =
        findApiConfig(apiVersion).flatMap(PackageJson.ScriptApiConfig::getOpenApi);

    if (configuredFilenameOpt.isEmpty()) {
      return;
    }

    var configuredFilename = configuredFilenameOpt.get();
    final ChangedOpenApi diff;
    try {
      var configuredOpenApiPath = workspacePaths.buildDir().resolve(configuredFilenameOpt.get());
      var configuredOpenApiContent = Files.readString(configuredOpenApiPath);

      diff = OpenApiCompare.fromContents(configuredOpenApiContent, generatedOpenApi);

    } catch (Exception e) {
      throw errors.exception(
          "Failed to compare generated OpenAPI specification with %s: %s",
          configuredFilename, e.getMessage());
    }

    errors.checkFatal(
        !diff.isIncompatible(),
        "The generated OpenAPI specification is not backwards compatible with %s: %s",
        configuredFilename,
        renderOpenApiDiff(diff));
  }

  private String renderOpenApiDiff(ChangedOpenApi diff) {
    var output = new ByteArrayOutputStream();
    // The renderer closes the writer
    new ConsoleRender().render(diff, new OutputStreamWriter(output, StandardCharsets.UTF_8));

    return output.toString(StandardCharsets.UTF_8);
  }
}
