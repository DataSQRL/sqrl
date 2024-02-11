package com.datasqrl.hooks;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
import com.datasqrl.graphql.inference.GraphqlQueryBuilder;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator;
import com.datasqrl.graphql.inference.GraphqlSchemaValidator;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.inject.AutoBind;
import com.datasqrl.injector.PostcompileHook;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.SqrlSchema;

/**
 * Creates new table functions from the graphql schema
 */
@AutoBind(PostcompileHook.class)
public class GraphqlInferencePostcompileHook implements PostcompileHook {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final Map<String, Optional<String>> scriptFiles;
  private final ResourceResolver resourceResolver;
  private final CompilerConfiguration compilerConfig;
  private final ErrorCollector errorCollector;
  private final APIConnectorManager apiManager;
  private final ModuleLoader moduleLoader;

  @Inject
  public GraphqlInferencePostcompileHook(ExecutionPipeline pipeline,
      SqrlFramework framework, SqrlConfig config,
      ResourceResolver resourceResolver,
      CompilerConfiguration compilerConfig, ErrorCollector errorCollector,
      APIConnectorManager apiManager, ModuleLoader moduleLoader) {
    this.pipeline = pipeline;

    this.framework = framework;
    this.resourceResolver = resourceResolver;
    this.compilerConfig = compilerConfig;
    this.errorCollector = errorCollector;
    this.apiManager = apiManager;
    this.moduleLoader = moduleLoader;

    scriptFiles = ScriptConfiguration.getFiles(config);
    Preconditions.checkArgument(!scriptFiles.isEmpty());
  }

  @SneakyThrows
  public static String inferGraphQLSchema(SqrlSchema schema,
      boolean addArguments) {
    GraphQLSchema gqlSchema = new GraphqlSchemaFactory(schema, addArguments).generate();

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  @Override
  public void runHook() {
    throw new RuntimeException("tbd");
    //todo migrate graphql to add table functions
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

    Name graphqlName = Name.system(
        scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY).orElse("<schema>").split("\\.")[0]);

    Optional<APISource> apiSchemaOpt = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .map(file -> APISource.of(file, framework.getNameCanonicalizer(), resourceResolver));

    APISource apiSchema = apiSchemaOpt.orElseGet(() -> new APISource(graphqlName,
        inferGraphQLSchema(framework.getSchema(), compilerConfig.isAddArguments())));

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
  }

}
