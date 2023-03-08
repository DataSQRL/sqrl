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
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.spi.ScriptConfiguration;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
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
  public CompilerResult run(ErrorCollector errors, Path packageFile, boolean debug) {

    Preconditions.checkArgument(Files.isRegularFile(packageFile));

    Path buildDir = packageFile.getParent();
    GlobalCompilerConfiguration globalConfig = GlobalEngineConfiguration.readFrom(packageFile,
        GlobalCompilerConfiguration.class);
    CompilerConfiguration compilerConfig = globalConfig.initializeCompiler(errors);
    EngineSettings engineSettings = globalConfig.initializeEngines(errors);

    DebuggerConfig debugger = DebuggerConfig.NONE;
    if (debug) debugger = compilerConfig.getDebug().getDebugger();

    ModuleLoader moduleLoader = new ModuleLoaderImpl(new ObjectLoaderImpl(new FileResourceResolver(buildDir), errors));
    TableSink errorSink = loadErrorSink(moduleLoader, compilerConfig.getErrorSink(), errors);

    SqrlDIModule module = new SqrlDIModule(engineSettings.getPipeline(), debugger,
        moduleLoader,
        new ErrorSink(errorSink));
    Injector injector = Guice.createInjector(module);

    ScriptConfiguration script = globalConfig.getScript();
    Preconditions.checkArgument(script != null);
    Path mainScript = buildDir.resolve(script.getMain());
    Optional<Path> graphqlSchema = script.getOptGraphQL().map(file -> buildDir.resolve(file));

    SqrlPhysicalPlan planner = injector.getInstance(SqrlPhysicalPlan.class);
    Namespace ns = planner.plan(Files.readString(mainScript));

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

    OptimizedDAG dag = optimizeDag(pgSchemaBuilder.getApiQueries(), queryPlanner, ns);
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

  private OptimizedDAG optimizeDag(List<APIQuery> queries, SqrlQueryPlanner planner, Namespace ns) {
    DAGPlanner dagPlanner = new DAGPlanner(planner.createRelBuilder(), planner.getPlanner(),
        ns.getPipeline());
    CalciteSchema relSchema = planner.getSchema();
    return dagPlanner.plan(relSchema, queries, ns.getExports(), ns.getJars());
  }

  private RootGraphqlModel updateGraphqlPlan(RootGraphqlModel root,
      Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(OptimizedDAG dag, SqrlQueryPlanner planner,
      Namespace namespace,
      TableSink errorSink) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(planner.createRelBuilder(), errorSink);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  @SneakyThrows
  public static String inferOrGetSchema(SqrlSchema schema, Optional<Path> graphqlSchema) {
    if (graphqlSchema.isPresent()) {
      Preconditions.checkArgument(Files.isRegularFile(graphqlSchema.get()));
      return Files.readString(graphqlSchema.get());
    }
    GraphQLSchema gqlSchema = new SchemaGenerator().generate(schema);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(gqlSchema);
  }

}
