package com.datasqrl.compile;

import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.GraphqlPostplanHook;
import com.datasqrl.actions.InferGraphqlSchema;
import com.datasqrl.actions.WriteDag;
import com.datasqrl.actions.WriteDagOld;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.v2.dag.DAGBuilder;
import com.datasqrl.v2.dag.DAGPlanner;
import com.datasqrl.v2.dag.PipelineDAG;
import com.datasqrl.v2.SqlScriptPlanner;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.MainScript;
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
  private final ErrorCollector errors;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {

    Sqrl2FlinkSQLTranslator environment = new Sqrl2FlinkSQLTranslator(buildPath);
    planner.planMain(mainScript, environment);
    DAGBuilder dagBuilder = planner.getDagBuilder();
    PipelineDAG dag = dagPlanner.optimize(dagBuilder.getDag());
    System.out.println(dag);
    PhysicalPlan physicalPlan = dagPlanner.assemble(dag, environment);

    //TODO: generate indexes

    writeDeploymentArtifactsHook.run(dag);

    TestPlan testPlan = null;
    //There can only be a single server plan
    Optional<ServerPhysicalPlan> serverPlan = physicalPlan.getPlans(ServerPhysicalPlan.class).findFirst();
    /*
    TODO: The following needs updating. Remove the && false condition and:
    - infer GraphQL schema from serverPlan
    - walk the GraphQL schema to validate and generate queries/coordinates
    - create the RootGraphQL model and attach to serverPlan
     */
    if (serverPlan.isPresent() && false) {
      Optional<APISource> apiSource = graphqlSourceFactory.get();
      if (apiSource.isEmpty() || executionGoal == ExecutionGoal.TEST) { //Infer schema from functions
        //TODO: rewrite the following to use the functions from the serverPlan
        apiSource = inferencePostcompileHook.inferGraphQLSchema()
            .map(schemaString -> new APISourceImpl(Name.system("<generated-schema>"), schemaString));
      }
      assert apiSource.isPresent();

      //TODO: Validates and generates queries
      inferencePostcompileHook.validateAndGenerateQueries(apiSource.get(), null);
      //TODO: Generates RootGraphQLModel, use serverplan as argument only
      graphqlPostplanHook.updatePlan(apiSource, null);

      //create test artifact

      if (executionGoal == ExecutionGoal.TEST) {
        testPlan = testPlanner.generateTestPlan(apiSource.get(), testsPath);
      }
    }
    return Pair.of(physicalPlan, testPlan);
  }
}

