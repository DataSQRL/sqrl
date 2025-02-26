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
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.UnExecutableSchemaGenerator;
import java.nio.file.Path;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/** Creates new table functions from the GraphQL schema. */
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
    Optional<GraphQLSchema> gqlSchema = schemaFactory.generate(ExecutionGoal.COMPILE);

    SchemaPrinter.Options opts =
        SchemaPrinter.Options.defaultOptions()
            .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
            .includeDirectives(false);

    return gqlSchema.map(schema -> new SchemaPrinter(opts).print(schema));
  }

  public Optional<APISource> run(Optional<Path> testsPath) {
    if (!isServerStagePresent()) {
      return Optional.empty();
    }

    SchemaPrinter.Options opts = createSchemaPrinterOptions();
    Optional<APISource> apiSource = getApiSource(opts);

    if (apiSource.isEmpty()) {
      return handleNoApiSource(opts);
    }

    APISource apiSchema = apiSource.get();

    if (goal == ExecutionGoal.TEST) {
      apiSchema = mergeTestSchema(apiSchema, opts, testsPath);
    }

    ErrorCollector apiErrors = setErrorCollectorSchema(apiSchema, errorCollector);

    try {
      validateAndGenerateQueries(apiSchema, apiErrors);
    } catch (Exception e) {
      throw apiErrors.handle(e);
    }

    return Optional.of(apiSchema);
  }

  private ErrorCollector setErrorCollectorSchema(
      APISource apiSchema, ErrorCollector errorCollector) {
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

  // Retrieves the API source, either from the factory or by inferring the schema
  private Optional<APISource> getApiSource(SchemaPrinter.Options opts) {
    return graphqlSourceFactory
        .get()
        .or(
            () ->
                inferGraphQLSchema()
                    .map(schemaString -> new APISourceImpl(Name.system("<schema>"), schemaString)));
  }

  // Handles the case when no API source is found
  private Optional<APISource> handleNoApiSource(SchemaPrinter.Options opts) {
    if (goal == ExecutionGoal.TEST) {
      Optional<APISource> apiSource =
          schemaFactory
              .generate(ExecutionGoal.TEST)
              .map(gqlSchema -> new SchemaPrinter(opts).print(gqlSchema))
              .map(schemaString -> new APISourceImpl(Name.system("<schema>"), schemaString));

      if (apiSource.isPresent()) {
        ErrorCollector errors = setErrorCollectorSchema(apiSource.get(), errorCollector);
        validateAndGenerateQueries(apiSource.get(), errors);
      }
    }
    return Optional.empty();
  }

  // Merges the test schema into the API schema if the goal is TEST
  @SneakyThrows
  private APISource mergeTestSchema(
      APISource apiSchema, SchemaPrinter.Options opts, Optional<Path> testsPath) {
    Optional<GraphQLSchema> gqlSchema = schemaFactory.generate(ExecutionGoal.TEST);
    if (gqlSchema.isPresent()) {
      String testSchemaString = new SchemaPrinter(opts).print(gqlSchema.get());

      TypeDefinitionRegistry schemaDef = parser.parse(apiSchema.getSchemaDefinition());
      TypeDefinitionRegistry testDef = parser.parse(testSchemaString);

      // Merge Query type
      ObjectTypeDefinition mergedQuery = mergeQueryType(schemaDef, testDef, testsPath);

      // Remove overlapping types from testDef
      removeOverlappingTypes(schemaDef, testDef);

      // Replace Query type in schemaDef with mergedQuery
      schemaDef.remove(schemaDef.getType("Query").get());
      schemaDef.add(mergedQuery);

      // Merge the two TypeDefinitionRegistries
      TypeDefinitionRegistry mergedDef = schemaDef.merge(testDef);

      // Generate combined schema string
      String combinedSchema =
          new SchemaPrinter(opts)
              .print(UnExecutableSchemaGenerator.makeUnExecutableSchema(mergedDef));

      // Return new APISource with combined schema
      return apiSchema.clone(combinedSchema);
    }
    return apiSchema;
  }

  // Merges the Query types from schemaDef and testDef
  private ObjectTypeDefinition mergeQueryType(
      TypeDefinitionRegistry schemaDef, TypeDefinitionRegistry testDef, Optional<Path> testsPath) {
    ObjectTypeDefinition schemaQuery = (ObjectTypeDefinition) schemaDef.getType("Query").get();
    ObjectTypeDefinition testQuery = (ObjectTypeDefinition) testDef.getType("Query").get();

    List<FieldDefinition> mergedFields = new ArrayList<>();

    if (testsPath.isPresent()) {
      mergedFields.addAll(schemaQuery.getFieldDefinitions());
    }
    mergedFields.addAll(testQuery.getFieldDefinitions());

    return ObjectTypeDefinition.newObjectTypeDefinition()
        .name("Query")
        .fieldDefinitions(mergedFields)
        .build();
  }

  // Removes overlapping types from testDef that are already in schemaDef
  private void removeOverlappingTypes(
      TypeDefinitionRegistry schemaDef, TypeDefinitionRegistry testDef) {
    // Remove Query type from testDef
    testDef.remove(testDef.getType("Query").get());

    // Remove types that are already in schemaDef from testDef
    for (TypeDefinition<?> type : schemaDef.types().values()) {
      testDef.remove(type);
    }

    // Remove scalars that are already in schemaDef from testDef
    for (ScalarTypeDefinition scalar : schemaDef.scalars().values()) {
      testDef.remove(scalar);
    }
  }

  // Validates the schema and generates queries and subscriptions
  private void validateAndGenerateQueries(APISource apiSchema, ErrorCollector apiErrors) {
    GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(framework, apiManager);
    schemaValidator.validate(apiSchema, apiErrors);

    GraphqlQueryGenerator queryGenerator =
        new GraphqlQueryGenerator(
            framework.getCatalogReader().nameMatcher(),
            framework.getSchema(),
            new GraphqlQueryBuilder(
                framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)),
            apiManager);

    queryGenerator.walk(apiSchema);

    // Add queries to apiManager
    queryGenerator.getQueries().forEach(apiManager::addQuery);

    // Add subscriptions to apiManager
    final APISource source = apiSchema;
    queryGenerator
        .getSubscriptions()
        .forEach(
            subscription ->
                apiManager.addSubscription(
                    new APISubscription(subscription.getAbsolutePath().getFirst(), source),
                    subscription));
  }
}
