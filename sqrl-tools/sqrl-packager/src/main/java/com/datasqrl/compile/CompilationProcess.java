package com.datasqrl.compile;

import com.datasqrl.actions.DagWriter;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.PhysicalPlanRewriter;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.v2.SqlScriptPlanner;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.datasqrl.v2.dag.DAGPlanner;
import com.datasqrl.v2.graphql.GenerateCoords;
import com.datasqrl.v2.graphql.InferGraphqlSchema2;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_=@Inject)
public class CompilationProcess {

  private final SqlScriptPlanner planner;
  private final DAGPlanner dagPlanner;
  private final BuildPath buildPath;
  private final MainScript mainScript;
  private final GenerateCoords generateCoords;
  private final InferGraphqlSchema2 inferGraphqlSchema;
  private final DagWriter writeDeploymentArtifactsHook;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final ExecutionGoal executionGoal;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {

    var environment = new Sqrl2FlinkSQLTranslator(buildPath);
    planner.planMain(mainScript, environment);
    var dagBuilder = planner.getDagBuilder();
    var dag = dagPlanner.optimize(dagBuilder.getDag());
    var physicalPlan = dagPlanner.assemble(dag, environment);
    List<PhysicalPlanRewriter> rewriters = ServiceLoaderDiscovery.getAll(PhysicalPlanRewriter.class);
    physicalPlan = physicalPlan.applyRewriting(rewriters, environment);

    writeDeploymentArtifactsHook.run(dag);

    TestPlan testPlan = null;
    //There can only be a single server plan
    var serverPlan = physicalPlan.getPlans(ServerPhysicalPlan.class).findFirst();
    if (serverPlan.isPresent()) {
      var apiSource = graphqlSourceFactory.get();
      if (apiSource.isEmpty() || executionGoal == ExecutionGoal.TEST) { //Infer schema from functions
        apiSource = inferGraphqlSchema.inferGraphQLSchema(serverPlan.get())
            .map(schemaString -> new APISourceImpl(Name.system("<generated-schema>"), schemaString));
      } else {
        inferGraphqlSchema.validateSchema(apiSource.get(), serverPlan.get());
      }
      assert apiSource.isPresent();
      generateCoords.generateCoordsAndUpdateServerPlan(apiSource, serverPlan.get());

      //create test artifact
      if (executionGoal == ExecutionGoal.TEST) {
        var testPlanner = new TestPlanner(serverPlan.get().getFunctions());
        testPlan = testPlanner.generateTestPlan(apiSource.get(), testsPath);
      }
    }
    return Pair.of(physicalPlan, testPlan);
  }
}

