package com.datasqrl.compile;

import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.GraphqlPostplanHook;
import com.datasqrl.actions.InferGraphqlSchema;
import com.datasqrl.actions.WriteDeploymentArtifacts;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.validate.ScriptPlanner;
import com.google.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_=@Inject)
public class CompilationProcess {

  private final ScriptPlanner planner;
  private final ModuleLoader moduleLoader;
  private final APIConnectorManagerImpl apiConnectorManager;
  private final DAGPlanner dagPlanner;
  private final MainScript mainScript;
  private final PhysicalPlanner physicalPlanner;
  private final GraphqlPostplanHook graphqlPostplanHook;
  private final CreateDatabaseQueries createDatabaseQueries;
  private final InferGraphqlSchema inferencePostcompileHook;
  private final WriteDeploymentArtifacts writeDeploymentArtifactsHook;
//  private final FlinkSqlGenerator flinkSqlGenerator;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final GraphQLMutationExtraction graphQLMutationExtraction;
  private final ExecutionPipeline pipeline;
  private final TestPlanner testPlanner;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Path testsPath) {
    pipeline.getStage(Type.SERVER)
        .flatMap(p->graphqlSourceFactory.get())
        .ifPresent(graphQLMutationExtraction::analyze);

    ModuleLoader composite = ModuleLoaderComposite.builder()
        .moduleLoader(moduleLoader)
        .moduleLoader(apiConnectorManager.getModuleLoader())
        .build();

    planner.plan(mainScript, composite);
    postcompileHooks();
    Optional<APISource> source = inferencePostcompileHook.run();
    SqrlDAG dag = dagPlanner.planLogical();
    PhysicalDAGPlan dagPlan = dagPlanner.planPhysical(dag);

    PhysicalPlan physicalPlan = physicalPlanner.plan(dagPlan);
    Optional<RootGraphqlModel> model = graphqlPostplanHook.run(source, physicalPlan);

    //create test artifact
    TestPlan testPlan;
    if (Files.isDirectory(testsPath) && source.isPresent()) {
      testPlan = testPlanner.generateTestPlan(source.get(), testsPath);
    } else {
      testPlan = null;
    }
    writeDeploymentArtifactsHook.run(source, physicalPlan, dag);
    return Pair.of(physicalPlan, testPlan);
  }

  private void postcompileHooks() {
    createDatabaseQueries.run();
  }
}
