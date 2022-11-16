package ai.datasqrl.graphql.inference;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.engines.JDBCConfiguration;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  private Env env;

  @SneakyThrows
  public Pair<InferredSchema, List<APIQuery>> inferSchema(TestScript script, Path schemaPath) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());

    String schemaStr = Files.readString(schemaPath);
    env = resolve.planDag(session, ConfiguredSqrlParser.newParser(ErrorCollector.root())
            .parse(script.getScriptContent()));

    //Inference
    SchemaInference inference = new SchemaInference(schemaStr, env.getRelSchema(), env.getSession().getPlanner()
            .getRelBuilder(), env.getSession().getPlanner());
    InferredSchema inferredSchema = inference.accept();

    //Build queries
    PgBuilder pgBuilder = new PgBuilder(schemaStr,
            env.getRelSchema(),
            env.getSession().getPlanner().getRelBuilder(),
            env.getSession().getPlanner());

    Root root = inferredSchema.accept(pgBuilder, null);

    List<APIQuery> queries = pgBuilder.getApiQueries();
    return Pair.of(inferredSchema, queries);
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    List<APIQuery> queries = inferSchema(script,schemaPath).getValue();
    /// plan dag
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner());
    OptimizedDAG dag = dagPlanner.plan(env.getRelSchema(), queries, env.getExports(), env.getSession().getPipeline());

//    CalciteUtil.getTables(env.getRelSchema(), VirtualRelationalTable.class)
//            .stream().forEach(vt -> {
//              System.out.println(vt.getNameId() + ": " + CalciteWriter.toString(vt.getStatistic()));
//            });

    IndexSelector indexSelector = new IndexSelector(env.getSession().getPlanner(),
            IndexSelector.Config.builder().dialect(JDBCConfiguration.Dialect.POSTGRES).build());
    List<IndexCall> allIndexes = new ArrayList<>();
    for (OptimizedDAG.ReadQuery query : dag.getDatabaseQueries()) {
      List<IndexCall> indexCall = indexSelector.getIndexSelection(query);
      allIndexes.addAll(indexCall);
//      System.out.println(query.getRelNode().explain());
//      indexSelection.stream().map(IndexSelection::getName).forEach(System.out::println);

    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}