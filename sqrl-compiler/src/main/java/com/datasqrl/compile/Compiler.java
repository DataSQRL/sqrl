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
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.PgSchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.calcite.Planner;
import com.datasqrl.plan.calcite.PlannerFactory;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.local.generate.Resolve.Env;
import com.datasqrl.plan.local.generate.Session;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.spi.ManifestConfiguration;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.ScriptNode;

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

    Env env = resolve.planDag(session, scriptNode, scriptErrors);

    String gqlSchema = inferOrGetSchema(env, graphqlSchema);

    InferredSchema inferredSchema = new SchemaInference(gqlSchema, env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder())
        .accept();

    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(gqlSchema,
        env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder(),
        env.getSession().getPlanner());

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    OptimizedDAG dag = optimizeDag(pgSchemaBuilder.getApiQueries(), env);
    PhysicalPlan plan = createPhysicalPlan(dag, env, session);

    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    return new CompilerResult(root, gqlSchema, plan);
  }

  private Session createSession(ErrorCollector collector, EngineSettings engineSettings,
      DebuggerConfig debugger) {
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    Planner planner = new PlannerFactory(schema.plus()).createPlanner();
    return new Session(collector, planner, engineSettings.getPipeline(), debugger);
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

    Env env = resolve.planDag(session, scriptNode, scriptErrors);

    return inferOrGetSchema(env, Optional.empty());
  }

  @Value
  public class CompilerResult {

    RootGraphqlModel model;
    String graphQLSchema;
    PhysicalPlan plan;
  }

  private OptimizedDAG optimizeDag(List<APIQuery> queries, Env env) {
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner(),
        env.getSession().getPipeline());
    CalciteSchema relSchema = env.getRelSchema();
    return dagPlanner.plan(relSchema, queries, env.getExports());
  }

  private RootGraphqlModel updateGraphqlPlan(RootGraphqlModel root,
      Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(OptimizedDAG dag, Env env, Session s) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(s.getPlanner().getRelBuilder());
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  @SneakyThrows
  public static String inferOrGetSchema(Env env, Optional<Path> graphqlSchema) {
    if (graphqlSchema.isPresent()) {
      Preconditions.checkArgument(Files.isRegularFile(graphqlSchema.get()));
      return Files.readString(graphqlSchema.get());
    }
    GraphQLSchema schema = new SchemaGenerator().generate(env.getRelSchema());

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(schema);
  }

}
