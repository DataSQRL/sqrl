/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compile;

import com.datasqrl.frontend.SqrlDIModule;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalCompilerConfiguration;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.frontend.SqrlPhysicalPlan;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.PgSchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.*;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.spi.ScriptConfiguration;
import com.datasqrl.util.FileUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.net.URI;
import java.util.Set;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlSchema;

import javax.validation.constraints.NotEmpty;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class Compiler {

  /**
   * Processes all the files in the build directory and creates the execution artifacts
   *
   * @return
   */
  @SneakyThrows
  public CompilerResult run(ErrorCollector errors, ResourceResolver resourceResolver, boolean debug) {

    Optional<URI> packagePath = resourceResolver.resolveFile(NamePath.of("package.json"));

    GlobalCompilerConfiguration globalConfig = GlobalEngineConfiguration.readFrom(packagePath.get(),
        GlobalCompilerConfiguration.class);
    CompilerConfiguration compilerConfig = globalConfig.initializeCompiler(errors);
    EngineSettings engineSettings = globalConfig.initializeEngines(errors);

    DebuggerConfig debugger = DebuggerConfig.NONE;
    if (debug) debugger = compilerConfig.getDebug().getDebugger();

    ModuleLoader moduleLoader = new ModuleLoaderImpl(new ObjectLoaderImpl(resourceResolver, errors));
    TableSink errorSink = loadErrorSink(moduleLoader, compilerConfig.getErrorSink(), errors);

    SqrlDIModule module = new SqrlDIModule(engineSettings.getPipeline(), debugger,
        moduleLoader,
        new ErrorSink(errorSink));
    Injector injector = Guice.createInjector(module);

    ScriptConfiguration script = globalConfig.getScript();
    Preconditions.checkArgument(script != null);

    URI mainScript = resourceResolver
        .resolveFile(NamePath.of(script.getMain()))
        .orElseThrow();

    Optional<URI> graphqlSchema = script.getOptGraphQL()
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

    PhysicalDAGPlan dag = optimizeDag(pgSchemaBuilder.getApiQueries(), queryPlanner, ns,
        !(resourceResolver instanceof ClasspathResourceResolver), errors);
    PhysicalPlan plan = createPhysicalPlan(dag, queryPlanner, ns, errorSink);

    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    return new CompilerResult(root, gqlSchema, plan);
  }

  private TableSink loadErrorSink(ModuleLoader moduleLoader, @NonNull @NotEmpty String errorSinkName, ErrorCollector error) {
    NamePath sinkPath = NamePath.parse(errorSinkName);

    Optional<TableSink> errorSink = moduleLoader
        .getModule(sinkPath.popLast())
        .flatMap(m->m.getNamespaceObject(sinkPath.popLast().getLast()))
        .map(s -> ((DataSystemNsObject) s).getTable())
        .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), error))
        .map(tblConfig ->
            tblConfig.initializeSink(error, sinkPath, Optional.empty()));
    error.checkFatal(errorSink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
        "Cannot resolve table sink: %s", errorSink);

    return errorSink.get();
  }


  @Value
  public class CompilerResult {

    RootGraphqlModel model;
    String graphQLSchema;
    PhysicalPlan plan;
  }

  private PhysicalDAGPlan optimizeDag(List<APIQuery> queries, SqrlQueryPlanner planner, Namespace ns,
      boolean includeJars, ErrorCollector errors) {
    DAGPlanner dagPlanner = new DAGPlanner(planner.createRelBuilder(), ns.getSchema().getPlanner(),
        ns.getSchema().getPipeline(), errors);
    CalciteSchema relSchema = planner.getSchema();
    return dagPlanner.plan(relSchema, queries, ns.getExports(), includeJars ? ns.getJars() : Set.of());
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
