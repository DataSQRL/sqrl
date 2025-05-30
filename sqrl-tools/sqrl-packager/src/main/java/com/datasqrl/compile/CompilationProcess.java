/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.compile;

import com.datasqrl.actions.DagWriter;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APISourceImpl;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.PhysicalPlanRewriter;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.planner.SqlScriptPlanner;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.dag.DAGPlanner;
import com.datasqrl.planner.graphql.GenerateCoords;
import com.datasqrl.planner.graphql.InferGraphqlSchema;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_ = @Inject)
public class CompilationProcess {

  private final SqlScriptPlanner planner;
  private final DAGPlanner dagPlanner;
  private final BuildPath buildPath;
  private final MainScript mainScript;
  private final PackageJson config;
  private final GenerateCoords generateCoords;
  private final InferGraphqlSchema inferGraphqlSchema;
  private final DagWriter writeDeploymentArtifactsHook;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final ExecutionGoal executionGoal;
  private final ErrorCollector errors;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {

    var environment =
        new Sqrl2FlinkSQLTranslator(
            buildPath,
            (FlinkStreamEngine) planner.getStreamStage().getEngine(),
            config.getCompilerConfig());
    planner.planMain(mainScript, environment);
    var dagBuilder = planner.getDagBuilder();
    var dag = dagPlanner.optimize(dagBuilder.getDag());
    var physicalPlan = dagPlanner.assemble(dag, environment);
    List<PhysicalPlanRewriter> rewriters =
        ServiceLoaderDiscovery.getAll(PhysicalPlanRewriter.class);
    physicalPlan = physicalPlan.applyRewriting(rewriters, environment);

    writeDeploymentArtifactsHook.run(dag);

    TestPlan testPlan = null;
    // There can only be a single server plan
    var serverPlanOpt = physicalPlan.getPlans(ServerPhysicalPlan.class).findFirst();
    if (serverPlanOpt.isPresent()) {
      var serverPlan = serverPlanOpt.get();
      errors.checkFatal(
          !serverPlan.getFunctions().isEmpty(),
          ErrorCode.NO_API_ENDPOINTS,
          "The SQRL script defines %s functions and %s mutations - cannot define an API",
          serverPlan.getFunctions().size(),
          serverPlan.getMutations().size());
      var apiSource = graphqlSourceFactory.get();
      if (apiSource.isEmpty()
          || executionGoal == ExecutionGoal.TEST) { // Infer schema from functions
        apiSource =
            inferGraphqlSchema
                .inferGraphQLSchema(serverPlan)
                .map(
                    schemaString ->
                        new APISourceImpl(Name.system("<generated-schema>"), schemaString));
      } else {
        inferGraphqlSchema.validateSchema(apiSource.get(), serverPlan);
      }
      assert apiSource.isPresent();
      generateCoords.generateCoordsAndUpdateServerPlan(apiSource, serverPlan);

      // create test artifact
      if (executionGoal == ExecutionGoal.TEST) {
        var testPlanner = new TestPlanner(serverPlanOpt.get().getFunctions());
        testPlan = testPlanner.generateTestPlan(apiSource.get(), testsPath);
      }
    }
    return Pair.of(physicalPlan, testPlan);
  }
}
