/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compile;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.frontend.SqrlDIModule;
import com.datasqrl.frontend.SqrlPhysicalPlan;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.PgSchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
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
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APIConnectors;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
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

    ResourceResolver resourceResolver = new FileResourceResolver(buildDir);
    ModuleLoader moduleLoader = new ModuleLoaderImpl(new ObjectLoaderImpl(resourceResolver, errors));
    TableSink errorSink = loadErrorSink(moduleLoader, compilerConfig.getErrorSink(), errors);

    SqrlDIModule module = new SqrlDIModule(pipelineFactory.createPipeline(), debugger,
        moduleLoader, new ErrorSink(errorSink), errors);
    Injector injector = Guice.createInjector(module);

    SqrlPhysicalPlan planner = injector.getInstance(SqrlPhysicalPlan.class);
    GraphQLMutationExtraction preAnalysis = injector.getInstance(GraphQLMutationExtraction.class);
    APIConnectorManager apiManager = injector.getInstance(APIConnectorManagerImpl.class);

    Map<String, Optional<String>> scriptFiles = ScriptConfiguration.getFiles(config);
    Preconditions.checkArgument(!scriptFiles.isEmpty());
    URI mainScript = resourceResolver
        .resolveFile(NamePath.of(scriptFiles.get(ScriptConfiguration.MAIN_KEY).get()))
        .orElseThrow();
    Optional<APISource> apiSchemaOpt = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .map(file -> APISource.of(file, preAnalysis.getCanonicalizer(), resourceResolver));

    apiSchemaOpt.ifPresent(api -> preAnalysis.analyze(api, apiManager));


    Namespace ns = planner.plan(FileUtil.readFile(mainScript), List.of(apiManager.getAsModuleLoader()));

    APISource apiSchema = apiSchemaOpt.orElseGet(() ->
        new APISource(Name.system("schema"), inferGraphQLSchema(ns.getSchema())));

    SqrlSchema schema = injector.getInstance(SqrlSchema.class);
    SqrlQueryPlanner queryPlanner = injector.getInstance(SqrlQueryPlanner.class);

    InferredSchema inferredSchema = new SchemaInference(
        moduleLoader,
        apiSchema,
        schema,
        queryPlanner.createRelBuilder(),
        ns,
        apiManager)
        .accept();

    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(apiSchema,
        ns.getSchema(),
        queryPlanner.createRelBuilder(),
        queryPlanner,
        ns.getOperatorTable(),
        apiManager);

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    PhysicalDAGPlan dag = planner.planDag(ns, apiManager, root,
       false);
    PhysicalPlan plan = createPhysicalPlan(dag, queryPlanner, errorSink);

    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    CompilerResult result = new CompilerResult(root, apiSchema.getSchemaDefinition(), plan);
    writeDeployArtifacts(result, deployDir);
    writeSchema(buildDir, result.getGraphQLSchema());

    return result;
  }

  private TableSink loadErrorSink(ModuleLoader moduleLoader, @NonNull String errorSinkName, ErrorCollector error) {
    NamePath sinkPath = NamePath.parse(errorSinkName);
    return LoaderUtil.loadSink(sinkPath, error, moduleLoader);
  }

  @SneakyThrows
  public static String inferGraphQLSchema(SqrlSchema schema) {
    GraphQLSchema gqlSchema = new SchemaGenerator().generate(schema);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

  private PhysicalPlan createPhysicalPlan(PhysicalDAGPlan dag, SqrlQueryPlanner planner,
      TableSink errorSink) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(planner.createRelBuilder(), errorSink);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  private RootGraphqlModel updateGraphqlPlan(RootGraphqlModel root,
      Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
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
