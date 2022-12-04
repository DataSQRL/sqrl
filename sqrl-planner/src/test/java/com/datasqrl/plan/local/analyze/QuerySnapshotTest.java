package com.datasqrl.plan.local.analyze;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.calcite.util.RelToSql;
import com.datasqrl.plan.local.generate.Resolve.Env;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.C360;
import com.datasqrl.util.data.Retail;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class QuerySnapshotTest extends AbstractLogicalSQRLIT {

  public static final String IMPORTS = C360.BASIC.getImports().getScript() + "\n"
      + "Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC;\n";

  private TestDataset example = Retail.INSTANCE;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
  }

  private Env generate(ScriptNode node) {
    return resolve.planDag(session, node);
  }

  public RelNode generate(String init, String sql, boolean nested) {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
    SqrlParser parser = new SqrlParser();

    Env env = generate(parser.parse(init + sql));

    return env.getOps().get(env.getOps().size() - 1).getRelNode();
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
      "Orders.o2 := SELECT x.* FROM @ AS x;",
      "X := SELECT e.* FROM Orders.entries e ORDER BY e.discount DESC;",
      "Orders.o2 := SELECT @.* FROM Orders;",
      "D := SELECT e.parent.id FROM Orders.entries AS e;",
      "D := SELECT * FROM Orders.entries e INNER JOIN e.parent p WHERE e.parent.customerid = 0 AND p.customerid = 0;",
      "Product2 := SELECT _ingest_time + INTERVAL 2 YEAR AS x FROM Product;",
      "Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n"
          + "O2 := SELECT p.* FROM Orders.entries.product p;",
      "O2 := SELECT * FROM Orders.entries e INNER JOIN e.parent;",
      "X := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;",
      "Orders.products := SELECT p.* FROM @.entries e INNER JOIN Product AS p ON e.productid = p.productid;",
      "Orders.entries.x := SELECT @.parent.id, @.discount FROM @ AS x;",
      "Orders.entries.x := SELECT @.parent.id, @.discount FROM @ AS x WHERE @.parent.id = 1;",
      "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n",
      "Orders.entries.discount := SELECT coalesce(x.discount, 0.0) FROM @ AS x;\n",
      "Orders.entries.total := SELECT x.quantity * x.unit_price - x.discount FROM @ AS x;\n",
      "Orders._stats := SELECT SUM(quantity * unit_price - discount) AS total, sum(discount) AS total_savings, "
          + "                 COUNT(1) AS total_entries "
          + "                 FROM @.entries e\n",
      "Orders._stats := SELECT SUM(quantity * unit_price - discount) AS total, sum(discount) AS total_savings, \n"
          + "                 COUNT(1) AS total_entries \n"
          + "                 FROM @.entries e;\n",
      "Orders3 := SELECT * FROM Orders.entries.parent.entries;\n",
      "Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;\n"
          + "Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n"
          + "Customer.recent_products := SELECT e.productid, e.product.category AS category,\n"
          + "                                   sum(e.quantity) AS quantity, count(1) AS num_orders\n"
          + "                            FROM @.orders.entries AS e\n"
          + "                            WHERE e.parent.time > now() - INTERVAL 2 YEAR\n"
          + "                            GROUP BY productid, category ORDER BY count(1) DESC, quantity DESC;\n",
      "Orders3 := SELECT * FROM Orders.entries.parent.entries e WHERE e.parent.customerid = 100;\n",
      "Orders.biggestDiscount := JOIN @.entries e ORDER BY e.discount DESC;\n"
          + "Orders2 := SELECT * FROM Orders.biggestDiscount.parent e;\n",
      "Orders.entries2 := SELECT @.id, @.time FROM @.entries;\n",
      //Assure that added parent primary keys do not override the explicit aliases
      "Orders.entries2 := SELECT e.discount AS id FROM @.entries e GROUP BY e.discount;\n",
      "Orders.newid := COALESCE(customerid, id);\n",
      "Category := SELECT DISTINCT category AS name FROM Product;\n",
      "Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n"
          + "Orders.entries.dProduct := SELECT DISTINCT category AS name FROM @.product;\n",
      "Orders.x := SELECT * FROM @ JOIN Product ON true;\n",
      "Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n"
          + "Orders.entries.dProduct := SELECT unit_price, product.category, product.name FROM @;\n",
      "Orders.newid := SELECT NOW(), STRING_TO_TIMESTAMP(TIMESTAMP_TO_STRING(EPOCH_TO_TIMESTAMP(100))) FROM Orders;",
      "Orders.entries.x := SELECT e.parent.entries.parent.id, f.parent.entries.parent.customerid "
          + "FROM @.parent.entries e JOIN e.parent.entries.parent.entries f "
          + "WHERE f.parent.entries.parent.id = 2;",
      "CustomerWithPurchase := SELECT * FROM Customer\n"
          + "WHERE customerid IN (SELECT customerid FROM Orders.entries.parent)\n"
          + "ORDER BY name;",
      "CustomerNames := SELECT *\n"
          + "FROM Customer\n"
          + "ORDER BY (CASE\n"
          + "    WHEN name IS NULL THEN email\n"
          + "    ELSE name\n"
          + "END);",
      "Orders.x := SELECT x.* FROM @ JOIN @ AS x",
      "Orders.entries.discount := COALESCE(discount, 0.0);\n"
          + "Orders.entries.total := quantity * unit_price - discount;"
  );

  List<Integer> disabledTests = List.of(37, 38);

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
            if (disabledTests.contains(testNo)) {
              log.info("Test Disabled.");
              return;
            }
            RelNode relNode = generate(IMPORTS, sql, false);

            SnapshotTest.createOrValidateSnapshot(getClass().getName(), "Query" + testNo,
                sql + "\n\n" + relNode.explain() +
                    "\n\n" + RelToSql.convertToSql(relNode)
            );
          }));
    }

    return gen;
  }
}
