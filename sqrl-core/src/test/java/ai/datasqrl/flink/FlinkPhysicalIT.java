package ai.datasqrl.flink;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.analyze.ResolveTest;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.ResultSetPrinter;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.C360;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FlinkPhysicalIT extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;
  ErrorCollector error;
  private Resolve resolve;
  private Session session;
  private Planner planner;
  private JDBCConnectionProvider jdbc;
  private PhysicalPlanner physicalPlanner;
  private TestDataset example;


  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getFlinkWithDB(true));
    example = C360.INSTANCE;
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
    String script = example.getImports().toString();
    Map<String,ResultSet> results = process(script,List.of("customer","product","entries","orders"));
    validateRowCounts(C360.INSTANCE.getTableCounts(), results);
  }

  @Test
  @Disabled
  public void simpleColumnDefinition() {
    String script = "IMPORT ecommerce-data.Customer;\n"
            + "Customer.timestamp := EPOCH_TO_TIMESTAMP(lastUpdated);\n";
    Map<String,ResultSet> results = process(script,List.of("customer"));
    validateRowCounts(C360.INSTANCE.getTableCounts(), results);
  }

  @SneakyThrows
  private void validateRowCounts(Map<String,Integer> rowCounts, Map<String,ResultSet> results) {
    for (Map.Entry<String,ResultSet> res : results.entrySet()) {
      Integer numExpectedRows = rowCounts.get(res.getKey());
      assertNotNull(numExpectedRows);
      System.out.println("Results for table: " + res.getKey());
      int numRows = ResultSetPrinter.print(res.getValue(), System.out);
      assertEquals(numRows,numRows);
    }
  }

  @SneakyThrows
  private Map<String,ResultSet> process(String script, List<String> queryTables) {
    ScriptNode node = parse(script);
    Resolve.Env resolvedDag = resolve.planDag(session, node);
    DAGPlanner dagPlanner = new DAGPlanner(planner);
    //We add a scan query for every query table
    List<APIQuery> queries = new ArrayList<APIQuery>();
    CalciteSchema relSchema = resolvedDag.getRelSchema();
    queryTables.stream().map(t -> ResolveTest.getLatestTable(relSchema,t,VirtualRelationalTable.class))
            .map(t -> t.get()).forEach(vt -> {
      String tblName =  vt.getNameId();
      RelNode rel = planner.getRelBuilder().scan(tblName).build();
      queries.add(new APIQuery(tblName.substring(0,tblName.indexOf(Name.NAME_DELIMITER)), rel));
    });
    OptimizedDAG dag = dagPlanner.plan(relSchema,queries);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    Job job = executor.execute(physicalPlan);
    System.out.println("Started Flink Job: " + job.getExecutionId());
    Map<String,ResultSet> results = new HashMap<>();
    for (Map.Entry<APIQuery,QueryTemplate> query : physicalPlan.getDatabaseQueries().entrySet()) {
      System.out.println("Executing query: " + query.getValue().getSql());
      ResultSet resultSet = jdbc.getConnection().createStatement()
              .executeQuery(query.getValue().getSql());
      results.put(query.getKey().getNameId(),resultSet);
    }
    return results;
  }

  private ScriptNode parse(String query) {
    return parser.parse(query);
  }
}
