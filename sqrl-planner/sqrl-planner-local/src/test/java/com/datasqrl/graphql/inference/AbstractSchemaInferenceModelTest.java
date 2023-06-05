/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.IndexCall;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelector;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APIQuery;
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

  private Namespace ns;

  public AbstractSchemaInferenceModelTest(Namespace ns) {
    this.ns = ns;
  }

  public AbstractSchemaInferenceModelTest() {
  }

  @SneakyThrows
  public Pair<InferredSchema, List<APIQuery>> inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    String schemaStr = Files.readString(schemaPath);
    ns = plan(script.getScript());
    Triple<InferredSchema, RootGraphqlModel, List<APIQuery>> result = inferSchemaModelQueries(
        planner,
        schemaStr);
    return Pair.of(result.getLeft(), result.getRight());
  }

  private Triple<InferredSchema, RootGraphqlModel, List<APIQuery>> inferSchemaModelQueries(
      SqrlQueryPlanner planner, String schemaStr) {
    //Inference
    SchemaInference inference = new SchemaInference(null, "schema", schemaStr,
        planner.getSchema(),
        planner.createRelBuilder(), ns);
    InferredSchema inferredSchema = inference.accept();

    //Build queries
    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(schemaStr,
        planner.getSchema(),
        planner.createRelBuilder(),
        planner,
        ns.getOperatorTable());

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    List<APIQuery> queries = pgSchemaBuilder.getApiQueries();
    return Triple.of(inferredSchema, root, queries);
  }

  public Pair<RootGraphqlModel, List<APIQuery>> getModelAndQueries(SqrlQueryPlanner planner,
      String schemaStr) {
    Triple<InferredSchema, RootGraphqlModel, List<APIQuery>> result = inferSchemaModelQueries(
        planner, schemaStr);
    return Pair.of(result.getMiddle(), result.getRight());
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    List<APIQuery> queries = inferSchemaAndQueries(script, schemaPath).getValue();
    /// plan dag
    DAGPlanner dagPlanner = new DAGPlanner(planner.createRelBuilder(), ns.getSchema().getPlanner(),
        ns.getSchema().getPipeline(), Debugger.NONE, errors);
    PhysicalDAGPlan dag = dagPlanner.plan(ns.getSchema(), queries, ns.getExports(), ns.getJars(),
        ns.getUdfs(), null);

    IndexSelector indexSelector = new IndexSelector(ns.getSchema().getPlanner(),
        IndexSelectorConfigByDialect.of("POSTGRES"));
    List<IndexCall> allIndexes = new ArrayList<>();
    for (PhysicalDAGPlan.ReadQuery query : dag.getReadQueries()) {
      List<IndexCall> indexCall = indexSelector.getIndexSelection(query);
      allIndexes.addAll(indexCall);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}