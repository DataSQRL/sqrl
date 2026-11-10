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

import static com.datasqrl.server.openapi.OpenApiService.OPENAPI_JSON_ARTIFACT_NAME_SUFFIX_TEMPLATE;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.GraphqlSourceLoader;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.PhysicalPlanRewriter;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.planner.SqlScriptPlanner;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.dag.DAGPlanner;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.plan.MutationDatabase;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.server.ServerModelManager;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
@Lazy
@RequiredArgsConstructor
public class CompilationProcess {

  private final SqlScriptPlanner planner;
  private final DAGPlanner dagPlanner;
  private final WorkspacePaths workspacePaths;
  private final MainScript mainScript;
  private final PackageJson config;
  private final DagWriter dagWriter;
  private final GraphqlSourceLoader graphqlSourceLoader;
  private final ServerModelManager serverModelManager;
  private final ExecutionGoal executionGoal;
  private final ErrorCollector errors;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {
    try (var environment =
        new Sqrl2FlinkSQLTranslator(
            workspacePaths,
            (FlinkStreamEngine) planner.getStreamStage().engine(),
            config.getCompilerConfig())) {

      return executeCompilationInternal(testsPath, environment);
    }
  }

  private Pair<PhysicalPlan, TestPlan> executeCompilationInternal(
      Optional<Path> testsPath, Sqrl2FlinkSQLTranslator environment) {

    planner.planMain(mainScript, Optional.empty(), environment);
    var dagBuilder = planner.getDagBuilder();
    var dag = dagPlanner.optimize(dagBuilder.getDag());
    var physicalPlan = dagPlanner.assemble(dag, environment);
    var mutationDatabase = physicalPlan.getMutationDatabase(environment);

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

      var loadResult = graphqlSourceLoader.load(serverPlan);
      var apiVersions = new ArrayList<>(loadResult.apiVersions());

      loadResult.inferredSchema().ifPresent(dagWriter::writeInferredSchema);

      if (!apiVersions.isEmpty()) {
        Collections.sort(apiVersions);
        dagWriter.writeDataModelVisual(
            apiVersions.get(apiVersions.size() - 1).schema().getDefinition());
      }

      apiVersions.forEach(
          api -> {
            var model = serverModelManager.generateGraphQLModel(api, serverPlan);
            var generatedOpenApi =
                serverModelManager.generateOpenApiJson(api, model, serverPlan.getServerConfig());

            serverPlan.getModels().put(api.version(), model);
            serverPlan
                .getDeploymentArtifacts()
                .add(
                    new DeploymentArtifact(
                        String.format(OPENAPI_JSON_ARTIFACT_NAME_SUFFIX_TEMPLATE, api.version()),
                        generatedOpenApi));
          });
      if (loadResult.inferredSchema().isEmpty()) {
        warnUnservedEndpoints(dag, serverPlan.getServedFunctions());
      }

      // create test artifact
      if (executionGoal == ExecutionGoal.TEST) {
        var gqlGenerator = new GqlGenerator(serverPlan.getFunctions());
        var jdbcViews =
            physicalPlan
                .getPlans(JdbcPhysicalPlan.class)
                .map(p -> p.getStatementsForType(Type.VIEW))
                .findFirst()
                .orElse(List.of());

        var testPlanner = new TestPlanner(config, gqlGenerator, jdbcViews);
        testPlan = testPlanner.generateTestPlan(apiVersions, testsPath);
      }
    }

    var rewriters = ServiceLoaderDiscovery.getAll(PhysicalPlanRewriter.class);
    physicalPlan = physicalPlan.applyRewriting(rewriters, environment, errors);

    // Write out the DAG after the physical plan is final
    dagWriter.run(dag, physicalPlan, planner.getCompleteScript().toString(), mutationDatabase);

    // Read database file if configured and check compatibility
    mainScript
        .getMutationDatabase()
        .ifPresent(
            compareDb ->
                errors.checkFatal(
                    MutationDatabase.isBackwardsCompatible(
                        mutationDatabase, compareDb, environment, errors),
                    "The mutation tables defined by the script are not backwards compatible with the provided database. See warnings above for incompatibilities."));
    return Pair.of(physicalPlan, testPlan);
  }

  /**
   * Warns about the endpoints that no custom GraphQL schema uses, since they are still computed.
   * Endpoints on an analytic database, like Iceberg, are left out because their views are queried
   * directly.
   */
  private void warnUnservedEndpoints(PipelineDAG dag, Set<NamePath> servedFunctions) {
    var unserved =
        dag.allNodesByClass(TableFunctionNode.class)
            .filter(node -> !(node.getChosenStage().engine() instanceof AnalyticDatabaseEngine))
            .map(TableFunctionNode::getFunction)
            .filter(fn -> fn.getVisibility().isQueryable() && !fn.getVisibility().isTest())
            .map(SqrlTableFunction::getFullPath)
            .filter(path -> !servedFunctions.contains(path))
            .map(NamePath::getDisplay)
            .toList();
    if (!unserved.isEmpty()) {
      errors.warn(
          "No GraphQL schema uses these endpoints, but they are still computed and written to the"
              + " database or log: %s. Add them to a schema if the API should serve them. Otherwise,"
              + " remove them, start their names with `_`, or hint a table `/*+no_query */`.",
          String.join(", ", unserved));
    }
  }
}
