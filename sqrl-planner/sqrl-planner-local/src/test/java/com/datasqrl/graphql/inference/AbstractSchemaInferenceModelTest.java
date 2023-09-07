/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
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
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.util.TestScript;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  protected Namespace ns;

  public AbstractSchemaInferenceModelTest(Namespace ns) {
    this.ns = ns;
    this.errors = ErrorCollector.root();
  }

  public AbstractSchemaInferenceModelTest() {
    this(null);
  }

  @SneakyThrows
  public Pair<InferredSchema, APIConnectorManager> inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    String schemaStr = Files.readString(schemaPath);
    ns = plan(script.getScript());
    Triple<InferredSchema, RootGraphqlModel, APIConnectorManager> result = inferSchemaModelQueries(
        planner,
        schemaStr);
    return Pair.of(result.getLeft(), result.getRight());
  }

  public static Triple<InferredSchema, RootGraphqlModel, APIConnectorManager> inferSchemaModelQueries(
      SqrlQueryPlanner planner, String schemaStr) {
    APIConnectorManager apiManager = new MockAPIConnectorManager();
    APISource source = APISource.of(schemaStr);
    //Inference
//    ErrorPrinter errors = errors.withSchema("<schema>",source.getSchemaDefinition());
    SchemaInference inference = new SchemaInference(planner.getFramework(), "<schema>", null,source,
        planner.getSchema(),
        planner.createRelBuilder(), apiManager);
    InferredSchema inferredSchema;
    try {
      inferredSchema = inference.accept();
    } catch (Exception e) {
//      errors.handle(e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    //Build queries
    SchemaBuilder schemaBuilder = new SchemaBuilder(planner.getFramework(), source,
        planner.getSchema(),
        planner.createRelBuilder(),
        planner,
        planner.getFramework().getSqrlOperatorTable(), apiManager);

    RootGraphqlModel root = inferredSchema.accept(schemaBuilder, null);

    return Triple.of(inferredSchema, root, apiManager);
  }

  public Pair<RootGraphqlModel, APIConnectorManager> getModelAndQueries(SqrlQueryPlanner planner,
      String schemaStr) {
    Triple<InferredSchema, RootGraphqlModel, APIConnectorManager> result = inferSchemaModelQueries(
        planner, schemaStr);
    return Pair.of(result.getMiddle(), result.getRight());
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    APIConnectorManager apiManager = inferSchemaAndQueries(script, schemaPath).getValue();
    /// plan dag
    DAGPlanner dagPlanner = new DAGPlanner(planner.getFramework(),
        ns.getPipeline(), Debugger.NONE, errors);
    PhysicalDAGPlan dag = dagPlanner.plan(ns.getSchema(), apiManager, ns.getExports(), ns.getJars(),
        ns.getUdfs(), null);

    IndexSelector indexSelector = new IndexSelector(planner.getFramework(),
        IndexSelectorConfigByDialect.of("POSTGRES"));
    List<QueryIndexSummary> allIndexes = new ArrayList<>();
    for (PhysicalDAGPlan.ReadQuery query : dag.getReadQueries()) {
      List<QueryIndexSummary> queryIndexSummary = indexSelector.getIndexSelection(query);
      allIndexes.addAll(queryIndexSummary);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}