package com.datasqrl.compile;

import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.GraphqlPostplanHook;
import com.datasqrl.actions.InferGraphqlSchema;
import com.datasqrl.actions.WriteDag;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.flinkwrapper.SqlScriptPlanner;
import com.datasqrl.flinkwrapper.SqrlEnvironment;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_=@Inject)
public class CompilationProcessV2 {

  private final SqlScriptPlanner planner;
  private final ModuleLoader moduleLoader;
  private final APIConnectorManagerImpl apiConnectorManager;
  private final DAGPlanner dagPlanner;
  private final BuildPath buildPath;
  private final MainScript mainScript;
  private final PhysicalPlanner physicalPlanner;
  private final GraphqlPostplanHook graphqlPostplanHook;
  private final CreateDatabaseQueries createDatabaseQueries;
  private final InferGraphqlSchema inferencePostcompileHook;
  private final WriteDag writeDeploymentArtifactsHook;
  //  private final FlinkSqlGenerator flinkSqlGenerator;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final ExecutionGoal executionGoal;
  private final GraphQLMutationExtraction graphQLMutationExtraction;
  private final ExecutionPipeline pipeline;
  private final TestPlanner testPlanner;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {

    SqrlEnvironment environment = new SqrlEnvironment(buildPath, moduleLoader);
    planner.plan(mainScript, environment);
    return null;
//    postcompileHooks();
//    Optional<APISource> source = inferencePostcompileHook.run(testsPath);
//    SqrlDAG dag = dagPlanner.planLogical();
//    PhysicalDAGPlan dagPlan = dagPlanner.planPhysical(dag);
//
//    PhysicalPlan physicalPlan = physicalPlanner.plan(dagPlan);
//    graphqlPostplanHook.updatePlan(source, physicalPlan);
//
//    //create test artifact
//    TestPlan testPlan;
//    if (source.isPresent() && executionGoal == ExecutionGoal.TEST) {
//      testPlan = testPlanner.generateTestPlan(source.get(), testsPath);
//    } else {
//      testPlan = null;
//    }
//    writeDeploymentArtifactsHook.run(dag);
//    return Pair.of(physicalPlan, testPlan);
  }

  private void postcompileHooks() {
    createDatabaseQueries.run();
  }
}

