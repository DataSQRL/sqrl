package com.datasqrl.v2.graphql;

import java.util.Optional;

import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;

import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/**
 * Creates new table functions from the GraphQL schema.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema2 {

  private final ErrorCollector errorCollector;
  private final GraphqlSchemaFactory2 graphqlSchemaFactory;

  @SneakyThrows
  public Optional<String> inferGraphQLSchema(ServerPhysicalPlan serverPlan) {
    Optional<GraphQLSchema> gqlSchema = graphqlSchemaFactory.generate(serverPlan);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return gqlSchema.map(schema -> new SchemaPrinter(opts).print(schema));
  }


  private ErrorCollector createErrorCollectorWithSchema(APISource apiSource) {
    return errorCollector.withSchema(apiSource.getName().getDisplay(), apiSource.getSchemaDefinition());
  }

  // Validates the schema
  public void validateSchema(APISource apiSource, ServerPhysicalPlan serverPlan) {
    var schemaValidator = new GraphqlSchemaValidator2(serverPlan.getFunctions(), serverPlan.getMutations(),
        createErrorCollectorWithSchema(apiSource));
    schemaValidator.validate(apiSource);
  }
}
