package com.datasqrl.graphql.inference;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.config.provider.Dialect;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.plan.global.*;
import com.datasqrl.plan.local.generate.Resolve.Env;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.TestScript;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  private Env env;

  @SneakyThrows
  public Pair<InferredSchema, List<APIQuery>> inferSchemaAndQueries(TestScript script, Path schemaPath) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    String schemaStr = Files.readString(schemaPath);
    env = resolve.planDag(session, SqrlParser.newParser()
            .parse(script.getScript()));
    Triple<InferredSchema, RootGraphqlModel, List<APIQuery>> result = inferSchemaModelQueries(env, schemaStr);
    return Pair.of(result.getLeft(),result.getRight());
  }

  private static Triple<InferredSchema, RootGraphqlModel, List<APIQuery>> inferSchemaModelQueries(Env env, String schemaStr) {
    //Inference
    SchemaInference inference = new SchemaInference(schemaStr, env.getRelSchema(), env.getSession().getPlanner()
            .getRelBuilder());
    InferredSchema inferredSchema = inference.accept();

    //Build queries
    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(schemaStr,
            env.getRelSchema(),
            env.getSession().getPlanner().getRelBuilder(),
            env.getSession().getPlanner());

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    List<APIQuery> queries = pgSchemaBuilder.getApiQueries();
    return Triple.of(inferredSchema, root, queries);
  }

  public static Pair<RootGraphqlModel, List<APIQuery>> getModelAndQueries(Env env, String schemaStr) {
    Triple<InferredSchema, RootGraphqlModel, List<APIQuery>> result = inferSchemaModelQueries(env, schemaStr);
    return Pair.of(result.getMiddle(),result.getRight());
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    List<APIQuery> queries = inferSchemaAndQueries(script,schemaPath).getValue();
    /// plan dag
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner(), env.getSession().getPipeline());
    OptimizedDAG dag = dagPlanner.plan(env.getRelSchema(), queries, env.getExports());

    IndexSelector indexSelector = new IndexSelector(env.getSession().getPlanner(),
            IndexSelectorConfigByDialect.of(Dialect.POSTGRES));
    List<IndexCall> allIndexes = new ArrayList<>();
    for (OptimizedDAG.ReadQuery query : dag.getReadQueries()) {
      List<IndexCall> indexCall = indexSelector.getIndexSelection(query);
      allIndexes.addAll(indexCall);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}