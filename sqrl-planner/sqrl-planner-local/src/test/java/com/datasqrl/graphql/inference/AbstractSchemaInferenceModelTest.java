/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import static com.datasqrl.plan.SqrlOptimizeDag.extractFlinkFunctions;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelector;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.analyze.MockAPIConnectorManager;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.util.TestScript;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  public AbstractSchemaInferenceModelTest() {
    this.errors = ErrorCollector.root();
  }

  @SneakyThrows
  public Pair<InferredSchema, APIConnectorManager> inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory(), Optional.empty());
    String schemaStr = Files.readString(schemaPath);
    plan(script.getScript());
    Triple<InferredSchema, RootGraphqlModel, APIConnectorManager> result = inferSchemaModelQueries(
        schemaStr, framework, pipeline, errors);
    return Pair.of(result.getLeft(), result.getRight());
  }

  public static Triple<InferredSchema, RootGraphqlModel, APIConnectorManager> inferSchemaModelQueries(
      String schemaStr, SqrlFramework framework, ExecutionPipeline pipeline, ErrorCollector errors) {
    APISource source = APISource.of(schemaStr);
    //Inference
    SqrlSchemaForInference sqrlSchemaForInference = new SqrlSchemaForInference(framework.getSchema());

    MockAPIConnectorManager apiManager = new MockAPIConnectorManager(framework, pipeline);

    SchemaInference inference = new SchemaInference(framework, "<schema>", null,source,
        sqrlSchemaForInference,
        framework.getQueryPlanner().getRelBuilder(), apiManager);

    InferredSchema inferredSchema;
    try {
      inferredSchema = inference.accept();
    } catch (Exception e) {
      errors.withSchema("<schema>", schemaStr).handle(e);
      return null;
    }

    //Build queries
    SchemaBuilder schemaBuilder = new SchemaBuilder(source, apiManager);

    RootGraphqlModel root = inferredSchema.accept(schemaBuilder, null);

    return Triple.of(inferredSchema, root, apiManager);
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    APIConnectorManager apiManager = inferSchemaAndQueries(script, schemaPath).getValue();
    // plan dag
    PhysicalDAGPlan dag = DAGPlanner.plan(framework,
        apiManager, framework.getSchema().getExports(),
        framework.getSchema().getJars(), extractFlinkFunctions(framework.getSqrlOperatorTable()), null, pipeline,
        errors, debugger);

    IndexSelector indexSelector = new IndexSelector(framework,
        IndexSelectorConfigByDialect.of("POSTGRES"));
    List<QueryIndexSummary> allIndexes = new ArrayList<>();
    for (PhysicalDAGPlan.ReadQuery query : dag.getReadQueries()) {
      List<QueryIndexSummary> queryIndexSummary = indexSelector.getIndexSelection(query);
      allIndexes.addAll(queryIndexSummary);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}