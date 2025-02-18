package com.datasqrl.v2;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.GraphqlSchemaParser;
import com.datasqrl.graphql.inference.GraphqlQueryBuilder;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator;
import com.datasqrl.graphql.inference.GraphqlSchemaValidator;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;

import java.util.*;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/**
 * Creates new table functions from the GraphQL schema.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema2 {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final ErrorCollector errorCollector;
  private final APIConnectorManager apiManager;
  private final GraphqlSchemaFactory2 graphqlSchemaFactory;
  private final GraphqlSchemaParser parser;

  @SneakyThrows
  public Optional<String> inferGraphQLSchema() {
    Optional<GraphQLSchema> gqlSchema = graphqlSchemaFactory.generate();

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return gqlSchema.map(schema -> new SchemaPrinter(opts).print(schema));
  }


  //TODO wire the error collector up
  private ErrorCollector setErrorCollectorSchema(APISource apiSchema, ErrorCollector errorCollector) {
    return errorCollector.withSchema(
        apiSchema.getName().getDisplay(), apiSchema.getSchemaDefinition());
  }

  // Validates the schema and generates queries and subscriptions
  public void validateAndGenerateQueries(APISource apiSchema, ErrorCollector apiErrors) {
    GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(framework, apiManager);
    schemaValidator.validate(apiSchema, apiErrors);

    GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(
        framework.getCatalogReader().nameMatcher(),
        framework.getSchema(),
        new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)),
        apiManager
    );

    queryGenerator.walk(apiSchema);

    // Add queries to apiManager
    queryGenerator.getQueries().forEach(apiManager::addQuery);

    // Add subscriptions to apiManager
    final APISource source = apiSchema;
    queryGenerator.getSubscriptions().forEach(subscription ->
        apiManager.addSubscription(
            new APISubscription(subscription.getAbsolutePath().getFirst(), source), subscription)
    );
  }
}
