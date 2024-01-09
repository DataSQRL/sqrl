/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compile;

import com.datasqrl.DefaultFunctions;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.plan.SqrlOptimizeDag;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.plan.ScriptPlanner;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.graphql.inference.SchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
public class Compiler {
  public static final String DEFAULT_SERVER_MODEL = "-model.json";
  public static final String DEFAULT_SERVER_CONFIG = "-config.json";

  private final Deserializer writer = new Deserializer();

  private final ObjectWriter objWriter = SqrlObjectMapper.INSTANCE
      .writerWithDefaultPrettyPrinter();
  /**
   * Processes all the files in the build directory and creates the execution artifacts
   *
   * @return
   */
  @SneakyThrows
  public CompilerResult run(ErrorCollector errors, Path buildDir, boolean debug, Path deployDir) {
    if (Files.isDirectory(deployDir)) {
      FileUtils.cleanDirectory(deployDir.toFile());
    } else {
      Files.createDirectories(deployDir);
    }

    SqrlConfig config = SqrlConfigCommons.fromFiles(errors,buildDir.resolve(
        Packager.PACKAGE_FILE_NAME));

    CompilerConfiguration compilerConfig = CompilerConfiguration.fromRootConfig(config);
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);

    DebuggerConfig debugger = DebuggerConfig.NONE;
    if (debug) debugger = compilerConfig.getDebugger();

    NameCanonicalizer nameCanonicalizer = NameCanonicalizer.SYSTEM;
    ResourceResolver resourceResolver = new FileResourceResolver(buildDir);
    SqrlFramework framework = new SqrlFramework(SqrlRelMetadataProvider.INSTANCE,
        SqrlHintStrategyTable.getHintStrategyTable(), nameCanonicalizer);

    DefaultFunctions functions = new DefaultFunctions();
    functions.getDefaultFunctions()
        .forEach((key, value) -> framework.getSqrlOperatorTable().addFunction(key, value));

    ModuleLoader moduleLoader = new ModuleLoaderImpl(new ObjectLoaderImpl(resourceResolver, errors,
        new CalciteTableFactory(framework)));
    TableSink errorTableSink = loadErrorSink(moduleLoader, compilerConfig.getErrorSink(), errors);

    ExecutionPipeline pipeline = pipelineFactory.createPipeline();
    ErrorSink errorSink = new ErrorSink(errorTableSink);

    GraphQLMutationExtraction preAnalysis = new GraphQLMutationExtraction(framework.getTypeFactory(),
        nameCanonicalizer);
    APIConnectorManager apiManager = new APIConnectorManagerImpl(new CalciteTableFactory(framework),
        pipeline, errors, moduleLoader, framework.getTypeFactory());

    Map<String, Optional<String>> scriptFiles = ScriptConfiguration.getFiles(config);
    Preconditions.checkArgument(!scriptFiles.isEmpty());
    URI mainScript = resourceResolver
        .resolveFile(NamePath.of(scriptFiles.get(ScriptConfiguration.MAIN_KEY).get()))
        .orElseThrow();
    Optional<APISource> apiSchemaOpt = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .map(file -> APISource.of(file, preAnalysis.getCanonicalizer(), resourceResolver));

    apiSchemaOpt.ifPresent(api -> preAnalysis.analyze(api, apiManager));


    ModuleLoader updatedModuleLoader = ModuleLoaderComposite.builder()
          .moduleLoader(moduleLoader)
          .moduleLoader(apiManager.getAsModuleLoader())
          .build();
    ScriptPlanner.plan(FileUtil.readFile(mainScript), List.of(), framework, updatedModuleLoader,
        nameCanonicalizer, errors);

    SqrlSchemaForInference sqrlSchemaForInference = new SqrlSchemaForInference(framework.getSchema());

    Name graphqlName = Name.system(scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY).orElse("<schema>")
        .split("\\.")[0]);

    APISource apiSchema = apiSchemaOpt.orElseGet(() ->
        new APISource(graphqlName, inferGraphQLSchema(sqrlSchemaForInference, compilerConfig.isAddArguments())));

    errors = errors.withSchema(apiSchema.getName().getDisplay(), apiSchema.getSchemaDefinition());

    RootGraphqlModel root;
    //todo: move
    try {
      InferredSchema inferredSchema = new SchemaInference(
          framework,
          apiSchema.getName().getDisplay(),
          moduleLoader,
          apiSchema,
          sqrlSchemaForInference,
          framework.getQueryPlanner().getRelBuilder(),
          apiManager)
          .accept();

      SchemaBuilder schemaBuilder = new SchemaBuilder(apiSchema, apiManager);

      root = inferredSchema.accept(schemaBuilder, null);
    } catch (Exception e) {
      throw errors.handle(e);
    }

    PhysicalDAGPlan dag = SqrlOptimizeDag.planDag(framework, pipelineFactory.createPipeline(), apiManager, root,
       false,new Debugger(debugger, moduleLoader), errors);
    PhysicalPlan physicalPlan = createPhysicalPlan(dag, errorTableSink, framework);

    root = updateGraphqlPlan(framework.getQueryPlanner(), root, physicalPlan.getDatabaseQueries());

    CompilerResult result = new CompilerResult(root, apiSchema.getSchemaDefinition(), physicalPlan);
    writeDeployArtifacts(result, deployDir);
    writeSchema(buildDir, result.getGraphQLSchema());

    return result;
  }

  private TableSink loadErrorSink(ModuleLoader moduleLoader, @NonNull String errorSinkName, ErrorCollector error) {
    NamePath sinkPath = NamePath.parse(errorSinkName);
    return LoaderUtil.loadSink(sinkPath, error, moduleLoader);
  }

  @SneakyThrows
  public static String inferGraphQLSchema(SqrlSchemaForInference schema,
      boolean addArguments) {
    GraphQLSchema gqlSchema = new SchemaGenerator().generate(schema, addArguments);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  private PhysicalPlan createPhysicalPlan(PhysicalDAGPlan dag,
      TableSink errorSink, SqrlFramework framework) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(framework, errorSink);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  private RootGraphqlModel updateGraphqlPlan(QueryPlanner planner, RootGraphqlModel root,
      Map<IdentifiedQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries, planner);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  @Value
  public class CompilerResult {

    RootGraphqlModel model;
    String graphQLSchema;
    PhysicalPlan plan;
  }

  @SneakyThrows
  private void writeSchema(Path rootDir, String schema) {
    Path schemaFile = rootDir.resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(schemaFile);
    Files.writeString(schemaFile,
        schema, StandardOpenOption.CREATE);
  }

  @SneakyThrows
  public void writeDeployArtifacts(CompilerResult result, Path deployDir) {
    result.plan.writeTo(deployDir, new Deserializer());
  }

}
