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

import static com.datasqrl.graphql.GraphqlSchemaFactory.API_DIRECTIVE_NAME;

import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

/** Creates new table functions from the GraphQL schema. */
@Component
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema {

  private final ErrorCollector errorCollector;
  private final GraphqlSchemaFactory graphqlSchemaFactory;

  @SneakyThrows
  public String inferGraphQLSchema(ServerPhysicalPlan serverPlan) {
    GraphQLSchema gqlSchema = graphqlSchemaFactory.generate(serverPlan);

    SchemaPrinter.Options opts =
        SchemaPrinter.Options.defaultOptions()
            .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
            .includeDirectives(directiveName -> directiveName.equalsIgnoreCase(API_DIRECTIVE_NAME));

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  public void validateSchema(ApiSources api, ServerPhysicalPlan serverPlan) {
    var schemaValidator =
        new GraphqlSchemaValidator(
            serverPlan.getFunctions(),
            serverPlan.getMutations(),
            errorCollector.withScript(api.schema().getPath(), api.schema().getDefinition()));

    schemaValidator.validate(api.schema(), api.version());
  }
}
