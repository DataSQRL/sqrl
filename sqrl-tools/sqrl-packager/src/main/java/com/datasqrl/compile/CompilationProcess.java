package com.datasqrl.compile;

import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.GraphqlPostplanHook;
import com.datasqrl.actions.InferGraphqlSchema;
import com.datasqrl.actions.WriteDag;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.LogicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.plan.validate.ScriptPlanner;
import com.google.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_=@Inject)
public class CompilationProcess {
  private final ScriptPlanner planner; // 1.3 TRANSPILER: script parsing (antlr) and validation, and bookkeeping of the sqrlNodes (calcite sqlNodes) of the script inside a calcite schema.
                                       // 2. LOGICAL PLAN REWRITER: SQRLLogicalPlanRewriter is called in various places:
                                       //   - at table creation time by the ScriptPlanner
                                       //   - when dealing with exported stream tables in DAGPlanner physical plan step
                                       //   - when determining the materialized tables in the DAGPlanner physical plan step
                                       //   - when determining the table scans in the DAGPlanner physical plan step
                                       //   - in the stage analysis by the DAGPlanner (determine viable stages)

  private final ModuleLoader moduleLoader; // 1.2 TRANSPILER: load external modules source/sinks etc...
  private final APIConnectorManagerImpl apiConnectorManager; //manages source and sinks
  private final DAGPlanner dagPlanner; // 3. DAG PLANNER: planLogical (applies static cost factors based on table types and assigns tables to stages based on the cheapest cost)
                                        // 4. DAG PLANNER: planPhysical (
                                        //    detect scanned tables to materialize and generate the corresponding queries and indexes,
                                        //    generate write queries for written tables and put them to stream stage,
                                        //    put computed table to stream stage,
                                        //    merge all the stages to the physical plan
                                        //    )
  private final MainScript mainScript;
  private final PhysicalPlanner physicalPlanner; // 5. PHYSICAL PLANNER: match preliminary physical plan (step 4) stages to configured engines to generate the physical plan
                                                // that bookkeeps everything needed for the engine environment (topics for kafka, tablespace for postGre, flinkSQL compiled plan, ...)
  private final GraphqlPostplanHook graphqlPostplanHook; // 6. PHYSICAL PLANNER: walks the GraphQL schema to create the graphQL model - which encapsulates the GraphQL schema and entry points (queries, mutations, subscriptions) - using the physical plan and the database queries
  private final CreateDatabaseQueries createDatabaseQueries; // 1.4. TRANSPILER: Based on the logicalPlan (calcite schema) keep track of the APIQueries (wrapper that links together database query parameters, graphQL query and corresponding Calcite relNode in the logical plan) for the table functions.
                                                            // This is useful to be able to expose data through DataBase requests and/or GraphQL requests.
  private final InferGraphqlSchema inferencePostcompileHook; // 1.5. TRANSPILER: generate the GraphQL schema from the logicalPlan if the user has not provided one.
                                                            // Validate the schema and walk it to create APIQueries for managing graphQL endpoints (queries, mutations, subscriptions)
  private final WriteDag writeDeploymentArtifactsHook; // 7. PACKAGER: generate DAG artifacts and write them to the build dir
//  private final FlinkSqlGenerator flinkSqlGenerator;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final ExecutionGoal executionGoal;
  private final GraphQLMutationExtraction graphQLMutationExtraction; //1.1 TRANSPILER: If the user provided a GraphQL schema, extract the mutations defined in the schema as APISource (so that all GraphQL endpoints are APISources)
  private final ExecutionPipeline pipeline; // handle execution stages
  private final TestPlanner testPlanner;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {
    pipeline.getStage(Type.SERVER)
        .flatMap(p->graphqlSourceFactory.get())
        .ifPresent(graphQLMutationExtraction::analyze);

    ModuleLoader composite = ModuleLoaderComposite.builder()
        .moduleLoader(apiConnectorManager.getModuleLoader())
        .moduleLoader(moduleLoader)
        .build();

    planner.plan(mainScript, composite);
    postcompileHooks();
    Optional<APISource> source = inferencePostcompileHook.run(testsPath);
    SqrlDAG dag = dagPlanner.planLogical();
    PhysicalDAGPlan dagPlan = dagPlanner.planPhysical(dag);

    PhysicalPlan physicalPlan = physicalPlanner.plan(dagPlan);
    graphqlPostplanHook.updatePlan(source, physicalPlan);

    //create test artifact
    TestPlan testPlan;
    if (source.isPresent() && executionGoal == ExecutionGoal.TEST) {
      testPlan = testPlanner.generateTestPlan(source.get(), testsPath);
    } else {
      testPlan = null;
    }
    writeDeploymentArtifactsHook.run(dag);
    return Pair.of(physicalPlan, testPlan);
  }

  private void postcompileHooks() {
    createDatabaseQueries.run();
  }
}
