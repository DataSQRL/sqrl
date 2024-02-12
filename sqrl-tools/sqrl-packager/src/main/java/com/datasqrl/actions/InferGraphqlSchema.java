package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
import com.datasqrl.graphql.inference.GraphqlQueryBuilder;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator;
import com.datasqrl.graphql.inference.GraphqlSchemaValidator;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.graphql.ScriptConfiguration;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.plan.queries.APISourceImpl;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
<<<<<<< HEAD
import org.apache.calcite.jdbc.SqrlSchema;
=======
import org.apache.commons.lang3.tuple.Pair;
>>>>>>> 4ceb27ed0 (Add more di)

/**
 * Creates new table functions from the graphql schema
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final ScriptFiles scriptFiles;
  private final ResourceResolver resourceResolver;
  private final CompilerConfiguration compilerConfig;
  private final ErrorCollector errorCollector;
  private final APIConnectorManager apiManager;
  private final ModuleLoader moduleLoader;
  private final GraphqlSchemaFactory schemaFactory;
  private final GraphqlSourceFactory graphqlSourceFactory;

  @SneakyThrows
  public static String inferGraphQLSchema(SqrlSchema schema,
      boolean addArguments) {
    GraphQLSchema gqlSchema = new GraphqlSchemaFactory(schema, addArguments).generate();

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  public void run() {
    if (pipeline.getStage(Type.SERVER).isEmpty()) {
      if (pipeline.getStage(Type.DATABASE).isPresent()) {
        AtomicInteger i = new AtomicInteger();
        framework.getSchema().getTableFunctions()
            .forEach(t->apiManager.addQuery(new APIQuery(
                "query" + i.incrementAndGet(),
                NamePath.ROOT,
                framework.getQueryPlanner().expandMacros(t.getViewTransform().get()),
                List.of(),
                List.of(), false)));
      }

      return;
    }

    APISource apiSchema = graphqlSourceFactory.get()
        .orElseGet(() ->
            new APISourceImpl(Name.system("<schema>"),
                inferGraphQLSchema(framework.getSchema(),
                    compilerConfig.isAddArguments())));

    ErrorCollector apiErrors = errorCollector.withSchema(apiSchema.getName().getDisplay(),
        apiSchema.getSchemaDefinition());

    //todo: move
    try {
      GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(framework.getCatalogReader().nameMatcher(),
          framework.getSchema(), apiSchema, (new SchemaParser()).parse(apiSchema.getSchemaDefinition()), apiManager);

      schemaValidator.validate(apiSchema, errorCollector);

      GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(framework.getCatalogReader().nameMatcher(),
          framework.getSchema(),  (new SchemaParser()).parse(apiSchema.getSchemaDefinition()), apiSchema,
          new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)), apiManager);

      queryGenerator.walk();
      queryGenerator.getQueries().forEach(apiManager::addQuery);
      queryGenerator.getSubscriptions().forEach(s->apiManager.addSubscription(
          new APISubscription(s.getAbsolutePath().getFirst(), apiSchema), s));

    } catch (Exception e) {
      throw apiErrors.handle(e);
    }
    return;
  }
}
