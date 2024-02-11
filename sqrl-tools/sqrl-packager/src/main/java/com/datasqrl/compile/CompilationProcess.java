package com.datasqrl.compile;

import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.graphql.inference.GraphqlModelGenerator;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.hooks.AnalyzeGqlSchemaHook;
import com.datasqrl.hooks.DatabaseQueriesPostcompileHook;
import com.datasqrl.hooks.GraphqlInferencePostcompileHook;
import com.datasqrl.hooks.GraphqlPostplanHook;
import com.datasqrl.hooks.WriteDeploymentArtifacts;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.ScriptPlanner;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.google.inject.Inject;
import graphql.schema.idl.SchemaParser;
import java.util.Optional;

public class CompilationProcess {

  private final ScriptPlanner planner;
  private final DAGPlanner dagPlanner;
  private final MainScript mainScript;
  private final PhysicalPlanner physicalPlanner;
  private final GraphqlPostplanHook graphqlPostplanHook;
  private final AnalyzeGqlSchemaHook analyzeGqlSchemaHook;
  private final DatabaseQueriesPostcompileHook databaseQueriesPostcompileHook;
  private final GraphqlInferencePostcompileHook inferencePostcompileHook;
  private final WriteDeploymentArtifacts writeDeploymentArtifactsHook;

  @Inject
  public CompilationProcess(
      ScriptPlanner planner,
      DAGPlanner dagPlanner,
      MainScript mainScript,
      PhysicalPlanner physicalPlanner,
      GraphqlPostplanHook graphqlPostplanHook,
      AnalyzeGqlSchemaHook analyzeGqlSchemaHook,
      DatabaseQueriesPostcompileHook databaseQueriesPostcompileHook,
      GraphqlInferencePostcompileHook inferencePostcompileHook,
      WriteDeploymentArtifacts writeDeploymentArtifactsHook) {
    this.planner = planner;
    this.dagPlanner = dagPlanner;
    this.mainScript = mainScript;
    this.physicalPlanner = physicalPlanner;
    this.graphqlPostplanHook = graphqlPostplanHook;
    this.analyzeGqlSchemaHook = analyzeGqlSchemaHook;
    this.databaseQueriesPostcompileHook = databaseQueriesPostcompileHook;
    this.inferencePostcompileHook = inferencePostcompileHook;
    this.writeDeploymentArtifactsHook = writeDeploymentArtifactsHook;
  }

  public void executeCompilation() {
    precompileHooks();
    planner.plan(mainScript);
    postcompileHooks();
    inferencePostcompileHook.run();
    PhysicalDAGPlan dagPlan = dagPlanner.plan();

    PhysicalPlan physicalPlan = physicalPlanner.plan(dagPlan);
    Optional<RootGraphqlModel> model = graphqlPostplanHook.run(physicalPlan);
    writeDeploymentArtifactsHook.run(model, physicalPlan);
  }

  private void precompileHooks() {
    analyzeGqlSchemaHook.runHook();
  }

  private void postcompileHooks() {
    databaseQueriesPostcompileHook.runHook();
  }
}
