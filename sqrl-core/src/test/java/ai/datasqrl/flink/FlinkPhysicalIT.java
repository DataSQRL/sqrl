package ai.datasqrl.flink;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.ResultSetPrinter;
import ai.datasqrl.util.data.C360;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

class FlinkPhysicalIT extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;
  ErrorCollector error;
  private Resolve resolve;
  private Session session;
  private Planner planner;
  private JDBCConnectionProvider jdbc;
  private PhysicalPlanner physicalPlanner;


  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    planner = new PlannerFactory(
        CalciteSchema.createRootSchema(false, false).plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve();
    DatabaseConnectionProvider db = sqrlSettings.getDatabaseEngineProvider().getDatabase(EnvironmentConfiguration.MetaData.DEFAULT_DATABASE);
    jdbc = (JDBCConnectionProvider) db;

    physicalPlanner = new PhysicalPlanner(importManager, jdbc,
            sqrlSettings.getStreamEngineProvider().create());
  }

  @Test
  public void importTableTest() {
    process(imports().toString());

  }

  private StringBuilder imports() {
    StringBuilder builder = new StringBuilder();
    builder.append("IMPORT ecommerce-data.Customer;\n");
//    builder.append("IMPORT ecommerce-data.Orders;\n");
//    builder.append("IMPORT ecommerce-data.Product;\n");
    return builder;
  }

  @SneakyThrows
  private void process(String script) {
    ScriptNode node = parse(script);
    Resolve.Env resolvedDag = resolve.planDag(session, node);
    DAGPlanner dagPlanner = new DAGPlanner();
    //We add a scan query for every table in the API
    List<APIQuery> queries = new ArrayList<APIQuery>();
    CalciteSchema relSchema = resolvedDag.getRelSchema();
    CalciteUtil.getTables(relSchema, VirtualRelationalTable.class).forEach(vt -> {
      String tblName =  vt.getNameId();
      RelNode rel = planner.getRelBuilder().scan(tblName).build();
      queries.add(new APIQuery(tblName + "_query", rel));
    });
    OptimizedDAG dag = dagPlanner.plan(relSchema,planner,queries);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    Job job = executor.execute(physicalPlan);
    System.out.println("Started Flink Job: " + job.getExecutionId());

    for (QueryTemplate query : physicalPlan.getDatabaseQueries().values()) {
      ResultSet resultSet = jdbc.getConnection().createStatement()
              .executeQuery(query.getSql());

      System.out.println("Results for ["+query.getSql()+"]: ");
      ResultSetPrinter.print(resultSet, System.out);
    }
  }

  private ScriptNode parse(String query) {
    return parser.parse(query);
  }
}
