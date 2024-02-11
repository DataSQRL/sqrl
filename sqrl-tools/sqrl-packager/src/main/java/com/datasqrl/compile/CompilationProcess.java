package com.datasqrl.compile;

import com.datasqrl.actions.AnalyzeGqlSchema;
import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.FlinkSqlPostprocessor;
import com.datasqrl.actions.GraphqlPostplanHook;
import com.datasqrl.actions.InferGraphqlSchema;
import com.datasqrl.actions.WriteDeploymentArtifacts;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.ScriptPlanner;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class CompilationProcess {

  private final ScriptPlanner planner;
  private final DAGPlanner dagPlanner;
  private final MainScript mainScript;
  private final PhysicalPlanner physicalPlanner;
  private final GraphqlPostplanHook graphqlPostplanHook;
  private final AnalyzeGqlSchema analyzeGqlSchema;
  private final CreateDatabaseQueries createDatabaseQueries;
  private final InferGraphqlSchema inferencePostcompileHook;
  private final WriteDeploymentArtifacts writeDeploymentArtifactsHook;
  private final FlinkSqlPostprocessor flinkSqlPostprocessor;

  public void executeCompilation() {
    precompileHooks();
    planner.plan(mainScript);
    postcompileHooks();
    inferencePostcompileHook.run();
    PhysicalDAGPlan dagPlan = dagPlanner.plan();

    PhysicalPlan physicalPlan = physicalPlanner.plan(dagPlan);
    Optional<RootGraphqlModel> model = graphqlPostplanHook.run(physicalPlan);
    writeDeploymentArtifactsHook.run(model, physicalPlan);
    flinkSqlPostprocessor.run(physicalPlan);
  }

  private void precompileHooks() {
    analyzeGqlSchema.run();
  }

  private void postcompileHooks() {
    createDatabaseQueries.run();
  }
}
