/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compile;

import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalCompilerConfiguration;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.PgSchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.*;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.spi.ManifestConfiguration;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.ScriptNode;

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
  public CompilerResult run(ErrorCollector collector, Path packageFile, boolean debug) {
    Preconditions.checkArgument(Files.isRegularFile(packageFile));

    Path buildDir = packageFile.getParent();
    GlobalCompilerConfiguration globalConfig = GlobalEngineConfiguration.readFrom(packageFile,
        GlobalCompilerConfiguration.class);
    CompilerConfiguration compilerConfig = globalConfig.initializeCompiler(collector);
    EngineSettings engineSettings = globalConfig.initializeEngines(collector);

    DebuggerConfig debugger = DebuggerConfig.NONE;
    if (debug) debugger = compilerConfig.getDebug().getDebugger();

    TableSink errorSink = loadErrorSink(compilerConfig.getErrorSink(), collector, buildDir);

    Session session = createSession(collector, engineSettings, debugger);
    Resolve resolve = new Resolve(buildDir);

    ManifestConfiguration manifest = globalConfig.getManifest();
    Preconditions.checkArgument(manifest != null);
    Path mainScript = buildDir.resolve(manifest.getMain());
    Optional<Path> graphqlSchema = manifest.getOptGraphQL().map(file -> buildDir.resolve(file));

    String scriptStr = Files.readString(mainScript);

    ErrorCollector scriptErrors = collector.withFile(mainScript, scriptStr);
    ScriptNode scriptNode = SqrlParser.newParser()
        .parse(scriptStr, scriptErrors);

    Namespace ns = resolve.planDag(scriptNode, scriptErrors, new FileResourceResolver(buildDir), session);

    String gqlSchema = inferOrGetSchema(session.getSchema(), graphqlSchema);

    InferredSchema inferredSchema = new SchemaInference(gqlSchema, session.getSchema(),
        session.createRelBuilder())
        .accept();

    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(gqlSchema,
        session.getSchema(),
        session.createRelBuilder(),
        session, ns.getOperatorTable());

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    OptimizedDAG dag = optimizeDag(pgSchemaBuilder.getApiQueries(), session, ns);
    PhysicalPlan plan = createPhysicalPlan(dag, session, errorSink);

    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    return new CompilerResult(root, gqlSchema, plan);
  }

  private TableSink loadErrorSink(@NonNull @NotEmpty String errorSinkName, ErrorCollector error,
      Path buildDir) {
    //TODO: can we resuse this code from Resolve?
    ModuleLoader moduleLoader = new ModuleLoaderImpl(new StandardLibraryLoader(
        Map.of()), new ObjectLoaderImpl(new FileResourceResolver(buildDir), error));
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

  private Session createSession(ErrorCollector collector, EngineSettings engineSettings,
      DebuggerConfig debugger) {
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    return Session.createSession(collector, engineSettings.getPipeline(), debugger, schema);
  }

  @SneakyThrows
  public String generateSchema(ErrorCollector collector, Path packageFile) {
    Preconditions.checkArgument(Files.isRegularFile(packageFile));

    Path buildDir = packageFile.getParent();
    GlobalCompilerConfiguration globalConfig = GlobalEngineConfiguration.readFrom(packageFile,
        GlobalCompilerConfiguration.class);
    EngineSettings engineSettings = globalConfig.initializeEngines(collector);

    Session session = createSession(collector, engineSettings, DebuggerConfig.NONE);
    Resolve resolve = new Resolve(buildDir);

    ManifestConfiguration manifest = globalConfig.getManifest();
    Preconditions.checkArgument(manifest != null);
    Path mainScript = buildDir.resolve(manifest.getMain());

    String scriptStr = Files.readString(mainScript);
    ErrorCollector scriptErrors = collector.withFile(mainScript, scriptStr);

    ScriptNode scriptNode = SqrlParser.newParser()
        .parse(scriptStr, scriptErrors);

    Namespace ns = resolve.planDag(scriptNode, scriptErrors, new FileResourceResolver(buildDir),
        session);

    return inferOrGetSchema(session.getSchema(), Optional.empty());
  }

  @Value
  public class CompilerResult {

    RootGraphqlModel model;
    String graphQLSchema;
    PhysicalPlan plan;
  }

  private OptimizedDAG optimizeDag(List<APIQuery> queries, Session session, Namespace ns) {
    DAGPlanner dagPlanner = new DAGPlanner(session.createRelBuilder(), session.getRelPlanner(),
        session.getPipeline());
    CalciteSchema relSchema = session.getSchema();
    return dagPlanner.plan(relSchema, queries, ns.getExports(), ns.getJars());
  }

  private RootGraphqlModel updateGraphqlPlan(RootGraphqlModel root,
      Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(OptimizedDAG dag, Session s, TableSink errorSink) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(s.createRelBuilder(), errorSink);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  @SneakyThrows
  public static String inferOrGetSchema(SqrlCalciteSchema calciteSchema, Optional<Path> graphqlSchema) {
    if (graphqlSchema.isPresent()) {
      Preconditions.checkArgument(Files.isRegularFile(graphqlSchema.get()));
      return Files.readString(graphqlSchema.get());
    }
    GraphQLSchema schema = new SchemaGenerator().generate(calciteSchema);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(schema);
  }

}
