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
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.server.converter.GraphQLSchemaConverter;
import com.datasqrl.server.converter.GraphQLSchemaConverterConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import com.datasqrl.server.operation.ApiOperation;
import java.util.ArrayList;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

/** Generates the model for the server */
@Component
@AllArgsConstructor
public class GenerateServerModel {

  private final PackageJson configuration;
  private final ErrorCollector errorCollector;
  private final GraphQLSchemaConverter converter;

  /**
   * Generates the {@link RootGraphQLModel} from the server plan and defined operations
   *
   * @param api Contains the GraphQL schema and any defined operations
   * @param serverPlan The physical plan for the server with all function definitions
   * @return
   */
  public RootGraphQLModel generateGraphQLModel(ApiSources api, ServerPhysicalPlan serverPlan) {
    var graphqlModelGenerator =
        new GraphqlModelGenerator(
            serverPlan.getFunctions(), serverPlan.getMutations(), errorCollector);
    graphqlModelGenerator.walkAPISource(api.schema());
    var schema = StringSchema.builder().schema(api.schema().getDefinition()).build();
    var graphSchema = converter.getSchema(schema.getSchema());
    var apiConfig = configuration.getCompilerConfig().getApiConfig();
    var converterConfig =
        GraphQLSchemaConverterConfig.builder()
            .addPrefix(apiConfig.isAddOperationsPrefix())
            .maxDepth(apiConfig.getMaxResultDepth())
            .protocols(apiConfig.getProtocols())
            .build();
    var localErrors =
        errorCollector.withScript(api.schema().getPath(), api.schema().getDefinition());
    var definedOperations = new ArrayList<ApiOperation>();
    // First, convert all explicitly defined operations, preserving the original order
    for (var operationFile : api.operations()) {
      localErrors =
          errorCollector.withScript(operationFile.getPath(), operationFile.getDefinition());
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
    return RootGraphQLModel.builder()
        .queries(graphqlModelGenerator.getQueryCoords())
        .mutations(graphqlModelGenerator.getMutations())
        .subscriptions(graphqlModelGenerator.getSubscriptions())
        .operations(dedupedOperations)
        .schema(schema)
        .build();
  }
}
