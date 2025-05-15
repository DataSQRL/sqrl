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
package com.datasqrl.planner.graphql;

import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APISource;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/** Creates new table functions from the GraphQL schema. */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema2 {

  private final ErrorCollector errorCollector;
  private final GraphqlSchemaFactory2 graphqlSchemaFactory;

  @SneakyThrows
  public Optional<String> inferGraphQLSchema(ServerPhysicalPlan serverPlan) {
    Optional<GraphQLSchema> gqlSchema = graphqlSchemaFactory.generate(serverPlan);

    SchemaPrinter.Options opts =
        SchemaPrinter.Options.defaultOptions()
            .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
            .includeDirectives(false);

    return gqlSchema.map(schema -> new SchemaPrinter(opts).print(schema));
  }

  private ErrorCollector createErrorCollectorWithSchema(APISource apiSource) {
    return errorCollector.withSchema(
        apiSource.getName().getDisplay(), apiSource.getSchemaDefinition());
  }

  // Validates the schema
  public void validateSchema(APISource apiSource, ServerPhysicalPlan serverPlan) {
    var schemaValidator =
        new GraphqlSchemaValidator2(
            serverPlan.getFunctions(),
            serverPlan.getMutations(),
            createErrorCollectorWithSchema(apiSource));
    schemaValidator.validate(apiSource);
  }
}
