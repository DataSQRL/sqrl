package com.datasqrl.compile;

import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.DagWriter;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalPlanRewriter;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.v2.graphql.GenerateCoords;
import com.datasqrl.v2.graphql.InferGraphqlSchema2;
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
import java.util.List;
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
  private final GenerateCoords generateCoords;
  private final CreateDatabaseQueries createDatabaseQueries;
  private final InferGraphqlSchema2 inferGraphqlSchema;
  private final DagWriter writeDeploymentArtifactsHook;
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
    PhysicalPlan physicalPlan = dagPlanner.assemble(dag, environment);
    List<PhysicalPlanRewriter> rewriters = ServiceLoaderDiscovery.getAll(PhysicalPlanRewriter.class);
    physicalPlan = physicalPlan.applyRewriting(rewriters, environment);

    writeDeploymentArtifactsHook.run(dag);

    TestPlan testPlan = null;
    //There can only be a single server plan
    Optional<ServerPhysicalPlan> serverPlan = physicalPlan.getPlans(ServerPhysicalPlan.class).findFirst();
    /*
    TODO (Etienne): The following needs updating. Remove the && false condition and:
    - infer GraphQL schema from serverPlan
    - walk the GraphQL schema to validate and generate queries/coordinates
    - make sure we generate the right testplan
    - create the RootGraphQL model and attach to serverPlan
     */
    if (serverPlan.isPresent()) {
      Optional<APISource> apiSource = graphqlSourceFactory.get();
      if (apiSource.isEmpty() || executionGoal == ExecutionGoal.TEST) { //Infer schema from functions
        apiSource = inferGraphqlSchema.inferGraphQLSchema(serverPlan.get())
            .map(schemaString -> new APISourceImpl(Name.system("<generated-schema>"), schemaString));
      }
      assert apiSource.isPresent();
      //TODO re-enable
//      inferGraphqlSchema.validateSchema(apiSource.get(), serverPlan.get());
      generateCoords.generateCoordsAndUpdateServerPlan(apiSource, serverPlan.get());

      //create test artifact
      if (executionGoal == ExecutionGoal.TEST) {
        testPlan = testPlanner.generateTestPlan(apiSource.get(), testsPath);
      }
    }
    return Pair.of(physicalPlan, testPlan);
  }
}

