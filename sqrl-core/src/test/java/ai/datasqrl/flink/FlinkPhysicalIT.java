package ai.datasqrl.flink;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.scripts.ScriptBundle;
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
import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.C360;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

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
    initialize(IntegrationTestSettings.getFlinkWithDB(false));
    example = C360.BASIC;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    assertTrue(
            importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));
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
  public void tableImportTest() {
    String script = example.getImports().toString();
    Map<String,Integer> rowCounts = getImportRowCounts();
    Map<String,ResultSet> results = process(script,rowCounts.keySet());
    validateRowCounts(rowCounts, results);
  }

  private Map<String,Integer> getImportRowCounts() {
    Map<String,Integer> rowCounts = new HashMap<>(example.getTableCounts());
    rowCounts.put("entries",7);
    return rowCounts;
  }

  @Test
  public void tableColumnDefinitionTest() {
    ScriptBuilder builder = example.getImports();
    Map<String,Integer> rowCounts = getImportRowCounts();

    builder.add("EntryPrice := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e"); //This is line 4 in the script
    rowCounts.put("entryprice",rowCounts.get("entries"));

//    builder.add("Customer.timestamp := EPOCH_TO_TIMESTAMP(lastUpdated)");
//    builder.add("Customer.month := ROUND_TO_MONTH(ROUND_TO_MONTH(timestamp))");
//    builder.add("CustomerCopy := SELECT timestamp, month FROM Customer");
//    rowCounts.put("customercopy",rowCounts.get("customer"));


//    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid");
//    rowCounts.put("ordercustomer",5); //One order joins twice because customer record isn't deduplicated yet
//    builder.add("OrderCustomerInterval := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
//            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH");
//    rowCounts.put("ordercustomerinterval",4);
//    builder.add("HistoricOrders := SELECT * FROM Orders WHERE \"time\" > now() - INTERVAL 5 YEAR");
//    rowCounts.put("historicorders",rowCounts.get("orders"));
//    builder.add("RecentOrders := SELECT * FROM Orders WHERE \"time\" > now() - INTERVAL 1 WEEK");
//    rowCounts.put("recentorders",0);

    Map<String,ResultSet> results = process(builder.getScript(),rowCounts.keySet());

    validateRowCounts(rowCounts, results);
  }

  @Test
  public void joinTest() {
    ScriptBuilder builder = example.getImports();
    Map<String,Integer> rowCounts = getImportRowCounts();

    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid");
    rowCounts.put("ordercustomer",5); //One order joins twice because customer record isn't deduplicated yet
    builder.add("OrderCustomerInterval := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" <= c.\"_ingest_time\" AND o.\"time\" >= c.\"_ingest_time\" - INTERVAL 1 YEAR");
    rowCounts.put("ordercustomerinterval",5);

    Map<String,ResultSet> results = process(builder.getScript(),rowCounts.keySet());
    validateRowCounts(rowCounts, results);
  }

  @SneakyThrows
  private void validateRowCounts(Map<String,Integer> rowCounts, Map<String,ResultSet> results) {
    for (Map.Entry<String,ResultSet> res : results.entrySet()) {
      Integer numExpectedRows = rowCounts.get(res.getKey());
      assertNotNull(numExpectedRows);
      System.out.println("Results for table: " + res.getKey());
      int numRows = ResultSetPrinter.print(res.getValue(), System.out);
      assertEquals(numExpectedRows,numRows,res.getKey());
    }
  }

  @SneakyThrows
  private Map<String,ResultSet> process(String script, Collection<String> queryTables) {
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
