package ai.datasqrl.graphql.inference;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.provider.Dialect;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.global.*;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.TestScript;
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
    env = resolve.planDag(session, ConfiguredSqrlParser.newParser(ErrorCollector.root())
            .parse(script.getScript()));
    Triple<InferredSchema, Root, List<APIQuery>> result = inferSchemaModelQueries(env, schemaStr);
    return Pair.of(result.getLeft(),result.getRight());
  }

  private static Triple<InferredSchema, Root, List<APIQuery>> inferSchemaModelQueries(Env env, String schemaStr) {
    //Inference
    SchemaInference inference = new SchemaInference(schemaStr, env.getRelSchema(), env.getSession().getPlanner()
            .getRelBuilder());
    InferredSchema inferredSchema = inference.accept();

    //Build queries
    PgBuilder pgBuilder = new PgBuilder(schemaStr,
            env.getRelSchema(),
            env.getSession().getPlanner().getRelBuilder(),
            env.getSession().getPlanner());

    Root root = inferredSchema.accept(pgBuilder, null);

    List<APIQuery> queries = pgBuilder.getApiQueries();
    return Triple.of(inferredSchema, root, queries);
  }

  public static Pair<Root, List<APIQuery>> getModelAndQueries(Env env, String schemaStr) {
    Triple<InferredSchema, Root, List<APIQuery>> result = inferSchemaModelQueries(env, schemaStr);
    return Pair.of(result.getMiddle(),result.getRight());
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    List<APIQuery> queries = inferSchemaAndQueries(script,schemaPath).getValue();
    /// plan dag
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner());
    OptimizedDAG dag = dagPlanner.plan(env.getRelSchema(), queries, env.getExports(), env.getSession().getPipeline());

    IndexSelector indexSelector = new IndexSelector(env.getSession().getPlanner(),
            IndexSelector.Config.builder().dialect(Dialect.POSTGRES).build());
    List<IndexCall> allIndexes = new ArrayList<>();
    for (OptimizedDAG.ReadQuery query : dag.getDatabaseQueries()) {
      List<IndexCall> indexCall = indexSelector.getIndexSelection(query);
      allIndexes.addAll(indexCall);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}