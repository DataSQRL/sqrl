/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compile;

import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.frontend.SqrlDIModule;
import com.datasqrl.frontend.SqrlPhysicalPlan;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.PgSchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.resolver.ClasspathResourceResolver;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.util.FileUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;

@Slf4j
public class Compiler {

  /**
   * Processes all the files in the build directory and creates the execution artifacts
   *
   * @return
   */
  @SneakyThrows
  public CompilerResult run(ErrorCollector errors, Path buildDir, boolean debug) {
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
        moduleLoader,
        new ErrorSink(errorSink));
    Injector injector = Guice.createInjector(module);

    Map<String, Optional<String>> scriptFiles = ScriptConfiguration.getFiles(config);
    Preconditions.checkArgument(!scriptFiles.isEmpty());

    URI mainScript = resourceResolver
        .resolveFile(NamePath.of(scriptFiles.get(ScriptConfiguration.MAIN_KEY).get()))
        .orElseThrow();

    Optional<URI> graphqlSchema = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .flatMap(file -> resourceResolver.resolveFile(NamePath.of(file)));

    SqrlPhysicalPlan planner = injector.getInstance(SqrlPhysicalPlan.class);
    Namespace ns = planner.plan(FileUtil.readFile(mainScript));

    String gqlSchema = inferOrGetSchema(ns.getSchema(), graphqlSchema);

    SqrlSchema schema = injector.getInstance(SqrlSchema.class);
    SqrlQueryPlanner queryPlanner = injector.getInstance(SqrlQueryPlanner.class);

    InferredSchema inferredSchema = new SchemaInference(gqlSchema, schema,
        queryPlanner.createRelBuilder())
        .accept();

    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(gqlSchema,
        ns.getSchema(),
        queryPlanner.createRelBuilder(),
        queryPlanner,
        ns.getOperatorTable());

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    PhysicalDAGPlan dag = planner.planDag(ns, pgSchemaBuilder.getApiQueries(),
        !(resourceResolver instanceof ClasspathResourceResolver));
    PhysicalPlan plan = createPhysicalPlan(dag, queryPlanner, ns, errorSink);

    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    return new CompilerResult(root, gqlSchema, plan);
  }

  private TableSink loadErrorSink(ModuleLoader moduleLoader, @NonNull String errorSinkName, ErrorCollector error) {
    NamePath sinkPath = NamePath.parse(errorSinkName);
    return LoaderUtil.loadSink(sinkPath, error, moduleLoader);
  }

  @Value
  public class CompilerResult {

    RootGraphqlModel model;
    String graphQLSchema;
    PhysicalPlan plan;
  }

  private RootGraphqlModel updateGraphqlPlan(RootGraphqlModel root,
      Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(PhysicalDAGPlan dag, SqrlQueryPlanner planner,
      Namespace namespace,
      TableSink errorSink) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(planner.createRelBuilder(), errorSink);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  @SneakyThrows
  public static String inferOrGetSchema(SqrlSchema schema, Optional<URI> graphqlSchema) {
    if (graphqlSchema.isPresent()) {
      return FileUtil.readFile(graphqlSchema.get());
    }
    GraphQLSchema gqlSchema = new SchemaGenerator().generate(schema);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

}
