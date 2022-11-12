package ai.datasqrl.graphql.inference;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.IndexSelection;
import ai.datasqrl.plan.global.IndexSelector;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.global.OptimizedDAG.ReadQuery;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.data.Retail.RetailScriptNames;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaInferenceModelTest extends AbstractLogicalSQRLIT {
  private Retail example = Retail.INSTANCE;

  @BeforeEach
  public void setup() throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
  }

  private Env compile(String str) {
    return resolve.planDag(session, ConfiguredSqrlParser.newParser(ErrorCollector.root()).parse(str));
  }

  @SneakyThrows
  @Test
  public void testC360Inference() {
    Path schema = Path.of("src/test/resources/c360bundle/schema.full.graphqls");
    String schemaStr = new String(Files.readAllBytes(schema));
    Env env = compile(Files.readString(example.getScript(RetailScriptNames.FULL).getScript()));

    //Inference
    SchemaInference inference = new SchemaInference(schemaStr, env.getRelSchema(), env.getSession().getPlanner()
        .getRelBuilder(), env.getSession().getPlanner());
    InferredSchema inferredSchema = inference.accept();

    assertEquals(64, inferredSchema.getQuery().getFields().size());

    //Build queries
    PgBuilder pgBuilder = new PgBuilder(schemaStr,
        env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder(),
        env.getSession().getPlanner() );

    Root root = inferredSchema.accept(pgBuilder, null);

    List<APIQuery> queries = pgBuilder.getApiQueries();

    assertEquals(444, queries.size());
    /// plan dag
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner());
    OptimizedDAG dag = dagPlanner.plan(env.getRelSchema(), queries, env.getExports(), env.getSession().getPipeline());

    IndexSelector indexSelector = new IndexSelector(env.getSession().getPlanner());
    for (OptimizedDAG.ReadQuery query : dag.getDatabaseQueries()) {
      List<IndexSelection> indexSelection = indexSelector.getIndexSelection(query);
    }
  }
}