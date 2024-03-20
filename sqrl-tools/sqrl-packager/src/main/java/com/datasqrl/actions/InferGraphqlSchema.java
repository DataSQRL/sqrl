package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
import com.datasqrl.graphql.inference.GraphqlQueryBuilder;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator;
import com.datasqrl.graphql.inference.GraphqlSchemaValidator;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.plan.queries.APISourceImpl;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/**
 * Creates new table functions from the graphql schema
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final ErrorCollector errorCollector;
  private final APIConnectorManager apiManager;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final GraphqlSchemaFactory schemaFactory;

  @SneakyThrows
  public String inferGraphQLSchema() {
    GraphQLSchema gqlSchema = schemaFactory.generate();

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  public Optional<APISource> run() {
    if (pipeline.getStage(Type.SERVER).isEmpty()) {
      return Optional.empty();
    }

    APISource apiSchema = graphqlSourceFactory.get()
        .orElseGet(() ->
            new APISourceImpl(Name.system("<schema>"),
                inferGraphQLSchema()));

    ErrorCollector apiErrors = errorCollector.withSchema(apiSchema.getName().getDisplay(),
        apiSchema.getSchemaDefinition());

    //todo: move
    try {
      GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(framework, apiManager);

      schemaValidator.validate(apiSchema, errorCollector);

      GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(framework.getCatalogReader().nameMatcher(),
          framework.getSchema(),
          new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)), apiManager);

      queryGenerator.walk(apiSchema);
      queryGenerator.getQueries().forEach(apiManager::addQuery);
      queryGenerator.getSubscriptions().forEach(s->apiManager.addSubscription(
          new APISubscription(s.getAbsolutePath().getFirst(), apiSchema), s));

    } catch (Exception e) {
      throw apiErrors.handle(e);
    }
    return Optional.of(apiSchema);
  }
}
