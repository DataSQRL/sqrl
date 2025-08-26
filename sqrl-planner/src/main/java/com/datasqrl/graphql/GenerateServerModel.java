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
package com.datasqrl.graphql;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.CompilerApiConfig;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.converter.GraphQLSchemaConverter;
import com.datasqrl.graphql.converter.GraphQLSchemaConverterConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;

/** Generates the model for the server */
@AllArgsConstructor(onConstructor_ = @Inject)
public class GenerateServerModel {

  private final PackageJson configuration;
  private final ErrorCollector errorCollector;
  private final GraphQLSchemaConverter converter;

  /**
   * Generates the {@link RootGraphqlModel} from the server plan and defined operations
   *
   * @param graphqlSchema The defined or inferred GraphQL schema
   * @param serverPlan The physical plan for the server with all function definitions
   * @param operationFiles Any explicitly defined operations (empty if none)
   * @return
   */
  public RootGraphqlModel generateGraphQLModel(
      ApiSource graphqlSchema, ServerPhysicalPlan serverPlan, List<ApiSource> operationFiles) {
    var graphqlModelGenerator =
        new GraphqlModelGenerator(
            serverPlan.getFunctions(), serverPlan.getMutations(), errorCollector);
    graphqlModelGenerator.walkAPISource(graphqlSchema);
    var schema = StringSchema.builder().schema(graphqlSchema.getDefinition()).build();
    var graphSchema = converter.getSchema(schema.getSchema());
    CompilerApiConfig apiConig = configuration.getCompilerConfig().getApiConfig();
    GraphQLSchemaConverterConfig converterConfig =
        GraphQLSchemaConverterConfig.builder()
            .addPrefix(apiConig.isAddOperationsPrefix())
            .maxDepth(apiConig.getMaxResultDepth())
            .protocols(apiConig.getProtocols())
            .build();
    ErrorCollector localErrors =
        errorCollector.withScript(graphqlSchema.getPath(), graphqlSchema.getDefinition());
    List<ApiOperation> definedOperations = new ArrayList<>();
    // First, convert all explicitly defined operations, preserving the original order
    for (ApiSource operationFile : operationFiles) {
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
    if (apiConig.generateOperations()) {
      try {
        definedOperations.addAll(converter.convertSchema(converterConfig, graphSchema));
      } catch (Throwable e) {
        throw localErrors.handle(e);
      }
    }
    // Third, distincting preserves only the first operation by id
    List<ApiOperation> dedupedOperations = definedOperations.stream().distinct().toList();
    return RootGraphqlModel.builder()
        .queries(graphqlModelGenerator.getQueryCoords())
        .mutations(graphqlModelGenerator.getMutations())
        .subscriptions(graphqlModelGenerator.getSubscriptions())
        .operations(dedupedOperations)
        .schema(schema)
        .build();
  }
}
