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
import ai.datasqrl.physical.database.relational.QueryTemplate;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.analyze.ResolveTest;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.ResultSetPrinter;
import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.C360;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.datasqrl.util.data.C360.RETAIL_DIR_BASE;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkPhysicalIT extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;
  ErrorCollector error;
  private Resolve resolve;
  private Session session;
  private Planner planner;
  private JDBCConnectionProvider jdbc;
  private PhysicalPlanner physicalPlanner;
  private TestDataset example;

  private SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getFlinkWithDB(false));
    example = C360.BASIC;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().getBundle();
    assertTrue(
            importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));

    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    planner = new PlannerFactory(schema.plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve(RETAIL_DIR_BASE.resolve("build/"));
    DatabaseConnectionProvider db = sqrlSettings.getDatabaseEngineProvider().getDatabase(EnvironmentConfiguration.MetaData.DEFAULT_DATABASE);
    jdbc = (JDBCConnectionProvider) db;

    physicalPlanner = new PhysicalPlanner(importManager, jdbc,
            sqrlSettings.getStreamEngineProvider().create(), planner);
    this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
  }

  @Test
  public void tableImportTest() {
    String script = example.getImports().toString();
    validate(script, "customer","product","orders","entries");
  }


  @Test
  public void tableColumnDefinitionTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("EntryPrice := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e"); //This is line 4 in the script

    builder.add("Customer.timestamp := EPOCH_TO_TIMESTAMP(lastUpdated)");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY timestamp DESC");

    builder.add("Orders.col1 := (id + customerid)/2");
    builder.add("Orders.entries.discount2 := COALESCE(discount,0.0)");

    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid, o.col1, e.discount2 FROM Orders o JOIN o.entries e JOIN Customer c on o.customerid = c.customerid");

    validate(builder.getScript(),"entryprice","customer","orders","entries","ordercustomer");
  }

  @Test
  public void nestedAggregationandSelfJoinTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Orders");
    builder.add("IMPORT ecommerce-data.Customer TIMESTAMP EPOCH_TO_TIMESTAMP(lastUpdated) AS updateTime");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add("Orders.total := SELECT SUM(e.quantity * e.unit_price - e.discount) as price, COUNT(e.quantity) as num, SUM(e.discount) as discount FROM _.entries e");
    builder.add("OrdersInline := SELECT o.id, o.customerid, o.\"time\", t.price, t.num FROM Orders o JOIN o.total t");
//    builder.add("Customer.orders_by_day := SELECT o.\"time\", o.price, o.num FROM _ JOIN OrdersInline o ON o.customerid = _.customerid");
    builder.add("Customer.orders_by_hour := SELECT round_to_hour(o.\"time\") as hour, SUM(o.price) as total_price, SUM(o.num) as total_num FROM _ JOIN OrdersInline o ON o.customerid = _.customerid GROUP BY hour");
    validate(builder.getScript(),"customer","orders","ordersinline","orders_by_hour");
  }

  @Test
  public void joinTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Customer TIMESTAMP epoch_to_timestamp(lastUpdated) as updateTime"); //we fake that customer updates happen before orders
    builder.add("IMPORT ecommerce-data.Orders TIMESTAMP \"time\" AS rowtime");

    //Normal join
    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid");
    //Interval join
    builder.add("OrderCustomerInterval := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
            "AND o.rowtime >= c.updateTime AND o.rowtime <= c.updateTime + INTERVAL 1 YEAR");
    //Temporal join
    builder.add("CustomerDedup := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add("OrderCustomerDedup := SELECT o.id, c.name, o.customerid FROM Orders o JOIN CustomerDedup c on o.customerid = c.customerid");

    System.out.println(builder);
    validate(builder.getScript(),"ordercustomer","ordercustomerinterval","customerdedup","ordercustomerdedup");
  }

  @Test
  public void aggregateTest() {
    ScriptBuilder builder = example.getImports();
    //temporal state
    builder.append("OrderAgg1 := SELECT o.customerid as customer, round_to_hour(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    builder.append("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o");
    //time window
    builder.append("Ordertime1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    //now() and sliding window
    builder.append("OrderNow1 := SELECT o.customerid as customer, round_to_hour(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer, bucket");
    builder.append("OrderNow2 := SELECT round_to_hour(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY bucket");
    builder.append("OrderNow3 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer");
    builder.append("OrderAugment := SELECT o.id, o.\"time\", c.order_count FROM Orders o JOIN OrderNow3 c ON o.customerid = c.customer"); //will be empty because OrderNow3 has a timestamp greater than Order
    //state
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.append("agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");

    validate(builder.getScript(),"orderagg1", "orderagg2","ordertime1","ordernow1",
            "ordernow2","ordernow3","orderaugment","ordercustomer","agg1");
  }

  @Test
  public void filterTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("HistoricOrders := SELECT * FROM Orders WHERE \"time\" >= now() - INTERVAL 5 YEAR");
    builder.add("RecentOrders := SELECT * FROM Orders WHERE \"time\" >= now() - INTERVAL 1 SECOND");

    validate(builder.getScript(), "historicorders", "recentorders");
  }

  @Test
  public void topNTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("Customer.updateTime := epoch_to_timestamp(lastUpdated)");
    builder.add("CustomerDistinct := DISTINCT Customer ON customerid ORDER BY updateTime DESC;");
    builder.add("CustomerDistinct.recentOrders := SELECT o.id, o.time FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.\"time\" DESC LIMIT 10;");

    builder.add("CustomerId := SELECT DISTINCT customerid FROM Customer;");
    builder.add("CustomerOrders := SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");

    builder.add("CustomerDistinct.distinctOrders := SELECT DISTINCT o.id FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.id DESC LIMIT 10;");
    builder.add("CustomerDistinct.distinctOrdersTime := SELECT DISTINCT o.id, o.time FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.time DESC LIMIT 10;");

    builder.add("Orders := DISTINCT Orders ON id ORDER BY \"time\" DESC");

    validate(builder.getScript(),"customerdistinct","customerid","customerorders","distinctorders","distinctorderstime","orders","entries");
  }

  @Test
  public void setTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Customer TIMESTAMP epoch_to_timestamp(lastUpdated) as updateTime"); //we fake that customer updates happen before orders
    builder.add("IMPORT ecommerce-data.Orders");

    builder.add("CombinedStream := (SELECT o.customerid, o.\"time\" AS rowtime FROM Orders o)" +
            " UNION ALL " +
            "(SELECT c.customerid, c.updateTime AS rowtime FROM Customer c);");
//    builder.add("StreamCount := SELECT round_to_day(rowtime) as day, COUNT(1) as num FROM CombinedStream GROUP BY day");
    validate(builder.getScript(), "combinedstream");//,"streamcount");
  }


  private void validate(String script, String... queryTables) {
    validate(script,Arrays.asList(queryTables));
  }

  @SneakyThrows
  private void validate(String script, Collection<String> queryTables) {
    ScriptNode node = parse(script);
    Resolve.Env resolvedDag = resolve.planDag(session, node);
    DAGPlanner dagPlanner = new DAGPlanner(planner);
    //We add a scan query for every query table
    List<APIQuery> queries = new ArrayList<APIQuery>();
    CalciteSchema relSchema = resolvedDag.getRelSchema();
    queryTables.stream().map(t -> ResolveTest.getLatestTable(relSchema,t,VirtualRelationalTable.class)
                    .orElseThrow(() -> new IllegalArgumentException("No such table: " + t)))
            .forEach(vt -> {
      String tblName =  vt.getNameId();
      RelNode rel = planner.getRelBuilder().scan(tblName).build();
      queries.add(new APIQuery(tblName.substring(0,tblName.indexOf(Name.NAME_DELIMITER)), rel));
    });
    OptimizedDAG dag = dagPlanner.plan(relSchema,queries, session.getPipeline());
    snapshot.addContent(dag);
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    Job job = executor.execute(physicalPlan);
    System.out.println("Started Flink Job: " + job.getExecutionId());
    Map<String,ResultSet> results = new HashMap<>();
    for (APIQuery query : queries) {
      QueryTemplate template = physicalPlan.getDatabaseQueries().get(query);
      String sqlQuery = RelToSql.convertToSql(template.getRelNode());
      System.out.println("Executing query: " + sqlQuery);
      ResultSet resultSet = jdbc.getConnection().createStatement()
              .executeQuery(sqlQuery);
      results.put(query.getNameId(),resultSet);
      //Since Flink execution order is non-deterministic we need to sort results and remove uuid and ingest_time which change with every invocation
      String content = Arrays.stream(ResultSetPrinter.toLines(resultSet, s -> Stream.of("_uuid", "_ingest_time").noneMatch(p -> s.startsWith(p))))
              .sorted().collect(Collectors.joining(System.lineSeparator()));
      snapshot.addContent(content,query.getNameId(),"data");
    }
    snapshot.createOrValidate();
  }

  private ScriptNode parse(String query) {
    return parser.parse(query);
  }
}
