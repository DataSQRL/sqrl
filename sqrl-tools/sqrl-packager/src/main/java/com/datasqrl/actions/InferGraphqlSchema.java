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
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.plan.queries.APISourceImpl;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private final GraphqlSchemaParser parser;
  private final ExecutionGoal goal;

  @SneakyThrows
  public String inferGraphQLSchema() {
    GraphQLSchema gqlSchema = schemaFactory.generate(ExecutionGoal.COMPILE);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  public Optional<APISource> run(Optional<Path> testsPath) {
    if (pipeline.getStage(Type.SERVER).isEmpty()) {
      return Optional.empty();
    }

    APISource apiSchema = graphqlSourceFactory.get()
        .orElseGet(() ->
            new APISourceImpl(Name.system("<schema>"),
                inferGraphQLSchema()));

    // merge in test schema
    if (goal == ExecutionGoal.TEST) {
      GraphQLSchema gqlSchema = schemaFactory.generate(ExecutionGoal.TEST);
      if (gqlSchema != null) {
        SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
            .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
            .includeDirectives(false);
        String testSchema = new SchemaPrinter(opts).print(gqlSchema);

        TypeDefinitionRegistry schemaDef = parser.parse(apiSchema.getSchemaDefinition());

        TypeDefinitionRegistry testDef = parser.parse(testSchema);

        ObjectTypeDefinition schemaQuery = (ObjectTypeDefinition) schemaDef.getType("Query").get();
        ObjectTypeDefinition testQuery = (ObjectTypeDefinition) testDef.getType("Query").get();

        List<FieldDefinition> mergedFields = new ArrayList<>();
        if (testsPath.isPresent()) {
          mergedFields.addAll(schemaQuery.getFieldDefinitions());
        }
        mergedFields.addAll(testQuery.getFieldDefinitions());

        ObjectTypeDefinition mergedQuery = ObjectTypeDefinition.newObjectTypeDefinition()
            .name("Query")
            .fieldDefinitions(mergedFields)
            .build();

        schemaDef.remove(schemaDef.getType("Query").get());
        testDef.remove(testDef.getType("Query").get());
        for (Map.Entry<String, TypeDefinition> type: schemaDef.types().entrySet()) {
          testDef.remove(type.getValue());
        }
        for (Map.Entry<String, ScalarTypeDefinition> type: schemaDef.scalars().entrySet()) {
          testDef.remove(type.getValue());
        }

        schemaDef.add(mergedQuery);

        TypeDefinitionRegistry merge = schemaDef.merge(testDef);

        String combinedSchema = new SchemaPrinter(opts)
            .print(UnExecutableSchemaGenerator.makeUnExecutableSchema(merge));

        apiSchema = apiSchema.clone(combinedSchema);
      }
    }

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
      final APISource source = apiSchema;
      queryGenerator.getSubscriptions().forEach(s->apiManager.addSubscription(
          new APISubscription(s.getAbsolutePath().getFirst(), source), s));

    } catch (Exception e) {
      throw apiErrors.handle(e);
    }
    return Optional.of(apiSchema);
  }
}
