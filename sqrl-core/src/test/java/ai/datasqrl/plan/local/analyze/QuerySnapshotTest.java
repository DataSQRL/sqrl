package ai.datasqrl.plan.local.analyze;

import static ai.datasqrl.graphql.SchemaGeneratorTest.createOrValidateSnapshot;
import static ai.datasqrl.util.data.C360.RETAIL_DIR_BASE;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.data.C360;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class QuerySnapshotTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;

  ErrorCollector error;

  private Session session;
  private Resolve resolve;
  SqrlCalciteSchema schema;

  public static final String IMPORTS = C360.BASIC.getImports().getScript() + "\n"
      + "Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC;\n"
//      + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
//      + "Orders.orders2 := JOIN _ AS o "
//      + " INNER JOIN o.entries AS e "
//      + " INNER JOIN e.parent p ON p._uuid = e._uuid;\n"
//      + "Customer.orders := JOIN _ INNER JOIN Orders ON Orders.customerid = _.customerid;\n"
//      + "Product.order_entries := JOIN _ INNER JOIN Orders.entries e ON e.productid = _.productid;\n"

      ;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.BASIC;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().getBundle();
    Assertions.assertTrue(
        importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));
    schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    Planner planner = new PlannerFactory(schema.plus()).createPlanner();

    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve(RETAIL_DIR_BASE.resolve("build/"));
  }

  private Env generate(ScriptNode node) {
    return resolve.planDag(session, node);
  }

  public RelNode generate(String init, String sql, boolean nested) {
    Env env = generate(ConfiguredSqrlParser.newParser(ErrorCollector.root()).parse(init + sql));

    return env.getOps().get(env.getOps().size() - 1).getRelNode();
//
//    SqrlCalciteSchema rootSchema = env.getRelSchema();
//
//    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(rootSchema);
//    List<SqlNode> nodes = ConfiguredSqrlParser.newParser(ErrorCollector.root())
//        .parse(sql).getStatements();
//
//    Resolve
//    .get(0);
//    if (node.getNamePath().getNames().length > 1) {
//      sqrlValidator.assignmentPath = node.getNamePath().popLast().stream().map(e->e.getCanonical())
//          .collect(Collectors.toList());
//    }
//
//    SqlNode query = ((QueryAssignment)node).getQuery();
//
//    SqlNode validated = sqrlValidator.validate(query);
//
//    env.getSession().getPlanner().refresh();
//    env.getSession().getPlanner().setValidator(validated, sqrlValidator);
//
//    RelNode relNode = env.getSession().getPlanner().rel(validated).rel;
//
//    System.out.println(relNode.explain());
//    return relNode;
  }

  //Do not change order
  List<String> nonNestedImportTests = List.of(
      "X := SELECT * FROM Product;",
      "X := SELECT * FROM Orders;",
      "X := SELECT * FROM Customer;",
      //Nested paths
      "X := SELECT discount FROM Orders.entries;",
      //parent
      "X := SELECT * FROM Orders.entries.parent;",
      "X := SELECT e1.discount, e2.discount "
          + "FROM Orders.entries.parent AS p "
          + "INNER JOIN p.entries AS e1 "
          + "INNER JOIN p.entries AS e2;",
      "X := SELECT o.time, o2.time, o3.time "
          + "FROM Orders AS o "
          + "INNER JOIN Orders o2 ON o._uuid = o2._uuid "
          + "INNER JOIN Orders o3 ON o2._uuid = o3._uuid "
          + "INNER JOIN Orders o4 ON o3._uuid = o4._uuid;",
      "X := SELECT g.discount "
          + "FROM Orders.entries AS e "
          + "INNER JOIN Orders.entries AS f ON true "
          + "INNER JOIN Orders.entries AS g ON true;",
      "Orders.o2 := SELECT x.* FROM _ AS x;",
      "X := SELECT e.* FROM Orders.entries e ORDER BY e.discount DESC;",
      "Orders.o2 := SELECT _.* FROM Orders;",
      "D := SELECT e.parent.id FROM Orders.entries AS e;",
      "D := SELECT * FROM Orders.entries e INNER JOIN e.parent p WHERE e.parent.customerid = 0 AND p.customerid = 0;",
      "Product2 := SELECT _ingest_time + INTERVAL 2 YEAR AS x FROM Product;",
      "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
          + "O2 := SELECT p.* FROM Orders.entries.product p;",
      "O2 := SELECT * FROM Orders.entries e INNER JOIN e.parent;",
      "X := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;",
      "Orders.products := SELECT p.* FROM _.entries e INNER JOIN Product AS p ON e.productid = p.productid;",
      "Orders.entries.x := SELECT _.parent.id, _.discount FROM _ AS x;",
      "Orders.entries.x := SELECT _.parent.id, _.discount FROM _ AS x WHERE _.parent.id = 1;",
      "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n",
      "Orders.entries.discount := SELECT coalesce(x.discount, 0.0) FROM _ AS x;\n",
      "Orders.entries.total := SELECT x.quantity * x.unit_price - x.discount FROM _ AS x;\n",
      "Orders._stats := SELECT SUM(quantity * unit_price - discount) AS total, sum(discount) AS total_savings, "
          + "                 COUNT(1) AS total_entries "
          + "                 FROM _.entries e\n",
      "Orders._stats := SELECT SUM(quantity * unit_price - discount) AS total, sum(discount) AS total_savings, \n"
          + "                 COUNT(1) AS total_entries \n"
          + "                 FROM _.entries e;\n",
      "Orders3 := SELECT * FROM Orders.entries.parent.entries;\n",
      "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
          + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
          + "Customer.recent_products := SELECT e.productid, e.product.category AS category,\n"
          + "                                   sum(e.quantity) AS quantity, count(1) AS num_orders\n"
          + "                            FROM _.orders.entries AS e\n"
          + "                            WHERE e.parent.time > now() - INTERVAL 2 YEAR\n"
          + "                            GROUP BY productid, category ORDER BY count(1) DESC, quantity DESC;\n",
      "Orders3 := SELECT * FROM Orders.entries.parent.entries e WHERE e.parent.customerid = 100;\n",
      "Orders.biggestDiscount := JOIN _.entries e ORDER BY e.discount DESC LIMIT 1;\n"
          + "Orders2 := SELECT * FROM Orders.biggestDiscount.parent e;\n",
      "Orders.entries2 := SELECT _.id, _.time FROM _.entries;\n",
      //Assure that added parent primary keys do not override the explicit aliases
      "Orders.entries2 := SELECT e.discount AS id FROM _.entries e GROUP BY e.discount;\n",
      "Orders.newid := COALESCE(customerid, id);\n",
      "Category := SELECT DISTINCT category AS name FROM Product;\n",
      "Orders.entries.product := JOIN Product ON Product.productid = _.productid LIMIT 1;\n"
       + "Orders.entries.dProduct := SELECT DISTINCT category AS name FROM _.product LIMIT 1;\n",
      "Orders.x := SELECT * FROM _ JOIN Product ON true;\n"


//      "Orders3 := SELECT __a1.id\n"
//          + "FROM Orders.entries AS e\n"
//          + "INNER JOIN e.parent AS __a1;",
//      "Orders3 := SELECT p.* FROM Orders o "
//          + "INNER JOIN o.entries.parent p "
//          + ";",
//      "Orders.o2 := SELECT count(*) FROM Orders;",
//      "Orders.o2 := SELECT count(entries) FROM Orders;",
//      "Orders.entries.x := SELECT _.parent.id AS x, sum(_.discount) FROM _ GROUP BY x"

  );

  AtomicInteger cnt = new AtomicInteger();

  //extra TO_UTC / at zone test for null second arg
  @TestFactory
  Iterable<DynamicTest> testExpectedQueries() {
    List<DynamicTest> gen = new ArrayList<>();
    for (String sql : nonNestedImportTests) {
      int testNo = cnt.incrementAndGet();
      gen.add(DynamicTest.dynamicTest(String.format(
              "Query" + testNo),
          () -> {
            System.out.println(sql);
            RelNode relNode = generate(IMPORTS, sql, false);
            System.out.println(RelToSql.convertToSql(relNode));

//            String plan = RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT,
//                SqlExplainLevel.ALL_ATTRIBUTES);
            createOrValidateSnapshot(getClass().getName(), "Query" + testNo,
                sql + "\n\n" + relNode.explain() +
                    "\n\n" + RelToSql.convertToSql(relNode)
            );
          }));
    }

    return gen;
  }
}
