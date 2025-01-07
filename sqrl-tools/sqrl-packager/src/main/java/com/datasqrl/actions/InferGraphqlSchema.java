package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.GraphqlSchemaParser;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
import com.datasqrl.graphql.inference.GraphqlQueryBuilder;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator;
import com.datasqrl.graphql.inference.GraphqlSchemaValidator;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.nio.file.Path;
import java.util.*;

/**
 * Creates new table functions from the GraphQL schema.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final ErrorCollector errorCollector;
  private final APIConnectorManager apiManager;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final GraphqlSchemaFactory schemaFactory;
  private final GraphqlSchemaParser parser;
  private final ExecutionGoal goal;

  @SneakyThrows
  public Optional<String> inferGraphQLSchema() {
    Optional<GraphQLSchema> gqlSchema = schemaFactory.generate();

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return gqlSchema.map(schema -> new SchemaPrinter(opts).print(schema));
  }

  public Optional<APISource> run(Optional<Path> testsPath) {
    if (!isServerStagePresent()) {
      return Optional.empty();
    }

    SchemaPrinter.Options opts = createSchemaPrinterOptions();
    Optional<APISource> apiSource = getApiSource(goal);

    if (apiSource.isEmpty()) {
      return handleNoApiSource(opts);
    }

    APISource apiSchema = apiSource.get();

    ErrorCollector apiErrors = setErrorCollectorSchema(apiSchema, errorCollector);

    try {
      validateAndGenerateQueries(apiSchema, apiErrors);
    } catch (Exception e) {
      throw apiErrors.handle(e);
    }

    return Optional.of(apiSchema);
  }

  private ErrorCollector setErrorCollectorSchema(APISource apiSchema, ErrorCollector errorCollector) {
    return errorCollector.withSchema(
        apiSchema.getName().getDisplay(), apiSchema.getSchemaDefinition());
  }

  // Checks if the server stage is present in the execution pipeline
  private boolean isServerStagePresent() {
    return pipeline.getStage(Type.SERVER).isPresent();
  }

  // Creates the SchemaPrinter options
  private SchemaPrinter.Options createSchemaPrinterOptions() {
    return SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
  }

  private Optional<APISource> getApiSource(ExecutionGoal goal) {
    if (goal == ExecutionGoal.TEST) {
      return generateSchema();
    }

    return graphqlSourceFactory.get()
        .or(this::generateSchema);
  }

  private Optional<APISource> generateSchema() {
    return inferGraphQLSchema()
        .map(schemaString -> new APISourceImpl(Name.system("<schema>"), schemaString));
  }

  // Handles the case when no API source is found
  private Optional<APISource> handleNoApiSource(SchemaPrinter.Options opts) {
    if (goal == ExecutionGoal.TEST) {
      Optional<APISource> apiSource = schemaFactory.generate()
          .map(gqlSchema -> new SchemaPrinter(opts).print(gqlSchema))
          .map(schemaString -> new APISourceImpl(Name.system("<schema>"), schemaString));

      if (apiSource.isPresent()) {
        ErrorCollector errors = setErrorCollectorSchema(apiSource.get(), errorCollector);
        validateAndGenerateQueries(apiSource.get(), errors);
      }
    }
    return Optional.empty();
  }

  // Validates the schema and generates queries and subscriptions
  private void validateAndGenerateQueries(APISource apiSchema, ErrorCollector apiErrors) {
    GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(framework, apiManager);
    schemaValidator.validate(apiSchema, apiErrors);

    GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(
        framework.getCatalogReader().nameMatcher(),
        framework.getSchema(),
        new GraphqlQueryBuilder(framework, new SqlNameUtil(NameCanonicalizer.SYSTEM)),
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
