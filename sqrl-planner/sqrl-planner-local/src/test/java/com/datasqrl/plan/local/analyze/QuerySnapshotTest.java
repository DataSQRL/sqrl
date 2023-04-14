/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.analyze;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.rules.IdealExecutionStage;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import java.nio.file.Path;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;
import com.datasqrl.util.SqlNodePrinter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Slf4j
class QuerySnapshotTest extends AbstractLogicalSQRLIT {
  private Retail example = Retail.INSTANCE;

  protected SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo info) {
    initialize(IntegrationTestSettings.builder()
            .stream(IntegrationTestSettings.StreamEngine.FLINK)
            .database(DatabaseEngine.INMEMORY).build(),
        (Path)null);

    snapshot = SnapshotTest.Snapshot.of(getClass(), info);
  }

  protected void validateScriptInvalid(String script) {
    try {
      Namespace ns = plan(script);
      fail("Expected an exception but did not encounter one");
    } catch (CollectedException e) {
      snapshot.addContent(ErrorPrinter.prettyPrint(errors), "errors");
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unknown exception", e);
    }
    snapshot.createOrValidate();
  }

  protected void validateScript(String script) {
    Namespace ns = plan(script);
    SQRLConverter sqrlConverter = new SQRLConverter(planner.createRelBuilder());
    ns.getSchema().plus().getTableNames().stream()
        .map(n->ns.getSchema().getTable(n, false))
        .filter(f->f.getTable() instanceof ScriptRelationalTable)
        .sorted(Comparator.comparing(f->f.name))
        .forEach(t-> {
          ScriptRelationalTable table = (ScriptRelationalTable) t.getTable();
          SQRLConverter.Config config = table.getBaseConfig().stage(IdealExecutionStage.INSTANCE)
              .addTimestamp2NormalizedChildTable(false).build();
          snapshot.addContent(
              sqrlConverter.convert(table, config, false, errors).explain(),
              t.name);
        });
    ns.getSchema().getAllTables().stream()
        .flatMap(t->t.getAllRelationships())
        .sorted(Comparator.comparing(Field::getName))
        .filter(r->r.getJoin().isPresent())
            .forEach(r->
                snapshot.addContent(
                    SqlNodePrinter.printJoin(r.getJoin().get()), "join-declaration-" +
                    r.getName()));
    snapshot.createOrValidate();
  }

  @Test
  public void stringLibTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("IMPORT string.*");
    builder.add("X := SELECT toBase64(name) AS b64Name FROM Product");
    validateScript(builder.getScript());
  }

  @Test
  public void productTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT * FROM Product");
    validateScript(builder.getScript());
  }

  @Test
  public void differentGroupByFromSelectOrderTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT max(productid) as MAX, name, description "
        + " FROM Product"
        + " GROUP BY description, name");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT * FROM Orders");
    validateScript(builder.getScript());
  }

  @Test
  public void customerTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT * FROM Customer");
    validateScript(builder.getScript());
  }

  @Test
  public void nestedPathsTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT discount FROM Orders.entries");
    validateScript(builder.getScript());
  }

  @Test
  public void parentTest2() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT * FROM Orders.entries.parent");
    validateScript(builder.getScript());
  }

  @Test
  public void innerJoinTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT e1.discount, e2.discount FROM Orders.entries.parent AS p INNER JOIN p.entries AS e1 INNER JOIN p.entries AS e2");
    validateScript(builder.getScript());
  }

  @Test
  public void innerJoinTimeTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT o.time, o2.time, o3.time FROM Orders AS o INNER JOIN Orders o2 ON o._uuid = o2._uuid INNER JOIN Orders o3 ON o2._uuid = o3._uuid INNER JOIN Orders o4 ON o3._uuid = o4._uuid");
    validateScript(builder.getScript());
  }

  @Test
  public void innerJoinDiscountTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT g.discount FROM Orders.entries AS e INNER JOIN Orders.entries AS f ON true INNER JOIN Orders.entries AS g ON true");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersUUIDTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.o2 := SELECT x.* FROM @ AS x");
    validateScript(builder.getScript());
  }

  @Test
  public void orderDiscountDescTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X := SELECT e.* FROM Orders.entries e ORDER BY e.discount DESC");
    validateScript(builder.getScript());
  }

  @Test
  public void orderSelectTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.o2 := SELECT @.* FROM Orders");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentIdTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("D := SELECT e.parent.id FROM Orders.entries AS e");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentCustomerTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("D := SELECT * FROM Orders.entries e INNER JOIN e.parent p WHERE e.parent.customerid = 0 AND p.customerid = 0");
    validateScript(builder.getScript());
  }

  @Test
  public void productIntervalTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Product2 := SELECT _ingest_time + INTERVAL 365 DAYS AS x FROM Product");
    validateScript(builder.getScript());
  }

  @Test
  public void productJoinTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentIdDiscountTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.x := SELECT @.parent.id, @.discount FROM @ AS x");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentIdDiscountConditionTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.x := SELECT @.parent.id, @.discount FROM @ AS x WHERE @.parent.id = 1");
    validateScript(builder.getScript());
  }

  @Test
  public void customerDistinctTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC");
    validateScript(builder.getScript());
  }

  @Test
  public void orderCoalesceTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.discount := SELECT coalesce(x.discount, 0.0) AS discount FROM @ AS x");
    validateScript(builder.getScript());
  }

  @Test
  public void orderTotalTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.total := SELECT x.quantity * x.unit_price - x.discount AS total FROM @ AS x");
    validateScript(builder.getScript());
  }

  @Test
  public void orderStatsTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders._stats := SELECT SUM(quantity * unit_price - discount) AS total, sum(discount) AS total_savings, COUNT(1) AS total_entries FROM @.entries e");
    validateScript(builder.getScript());
  }

  @Test
  public void orderStatsNestedTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders._stats := SELECT SUM(quantity * unit_price - discount) AS total, sum(discount) AS total_savings, COUNT(1) AS total_entries FROM @.entries e");
    validateScript(builder.getScript());
  }

  @Test
  public void orders3Test() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders3 := SELECT * FROM Orders.entries.parent.entries;");
    validateScript(builder.getScript());
  }

  @Test
  public void customerOrdersTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;\n"
        + "Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void customerRecentProductsTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;");
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;");
    builder.add("Customer.recent_products := SELECT e.productid, e.product.category AS category,\n"
        + "                                       sum(e.quantity) AS quantity, count(1) AS num_orders\n"
        + "                                FROM @.orders.entries AS e\n"
        + "                                WHERE e.parent.time > now() - INTERVAL 365 DAYS\n"
        + "                                GROUP BY productid, category ORDER BY count(1) DESC, quantity DESC;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void orders2Test() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders3 := SELECT * FROM Orders.entries.parent.entries e WHERE e.parent.customerid = 100;\n"
        + "Orders.biggestDiscount := JOIN @.entries e ORDER BY e.discount DESC;\n"
        + "Orders2 := SELECT * FROM Orders.biggestDiscount.parent e;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersEntriesTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries2 := SELECT @.id, @.time FROM @.entries;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersEntriesDiscountTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.discount := COALESCE(discount, 0.0);\n"
        + "Orders.entries.total := quantity * unit_price - discount;");
    validateScript(builder.getScript());
  }

  @Test
  public void categoryTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Category := SELECT DISTINCT category AS name FROM Product;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersEntriesProductTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n"
        + "Orders.entries.dProduct := SELECT DISTINCT category AS name FROM @.product;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersXTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.x := SELECT * FROM @ JOIN Product ON true;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersEntriesXTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid LIMIT 1;\n"
        + "Orders.entries.dProduct := SELECT unit_price, product.category, product.name FROM @;\n");
    validateScript(builder.getScript());
  }

  @Test
  @Disabled
  public void ordersNewIdTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.newid := SELECT NOW(), STRING_TO_TIMESTAMP(TIMESTAMP_TO_STRING(EPOCH_TO_TIMESTAMP(100))) FROM Orders;");
    validateScript(builder.getScript());
  }

  @Test
  public void customerWithPurchaseTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("CustomerWithPurchase := SELECT * FROM Customer\n"
        + "WHERE customerid IN (SELECT customerid FROM Orders.entries.parent)\n"
        + "ORDER BY name;");
    validateScript(builder.getScript());
  }

  @Test
  public void customerNamesTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("CustomerNames := SELECT *\n"
        + "FROM Customer\n"
        + "ORDER BY (CASE\n"
        + "    WHEN name IS NULL THEN email\n"
        + "    ELSE name\n"
        + "END);");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersX2Test() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.x := SELECT x.* FROM @ JOIN @ AS x");
    validateScript(builder.getScript());
  }

  @Test
  public void testCatchingCalciteErrorTest() {
    validateScriptInvalid(""
        + "IMPORT ecommerce-data.Product;"
        //Expression 'productid' is not being grouped
        + "X := SELECT productid, SUM(productid) FROM Product GROUP BY name");
  }

  // IMPORTS
  @Test
  public void import1() {
    validateScript("IMPORT ecommerce-data.Orders;");
  }

  @Test
  public void import2() {
    validateScript("IMPORT ecommerce-data.*;");
  }

  @Test
  public void import3() {
    validateScript("IMPORT ecommerce-data.Orders AS O;");
  }

  @Test
  public void duplicateImportTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "IMPORT ecommerce-data.Product;\n");
  }

  @Test
  public void stringLiteral() {
    validateScript("IMPORT ecommerce-data.Product;"
        + "Product.url := 'test'");
  }

  @Test
  public void absoluteTest1() {
    validateScript("IMPORT ecommerce-data.Product;"
        + "X := SELECT productid FROM Product;");
  }

  @Test
  public void absoluteTest2() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT discount FROM Orders.entries;");
  }

  @Test
  public void relativeTest1() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.entries.d2 := SELECT @.discount FROM @;");
  }

  @Test
  public void relativeTest2() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.x := SELECT discount FROM @.entries;");
  }

  @Test
  public void noPathOrderByTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries e ORDER BY e.discount DESC;");
  }

  @Test
  public void assignmentHintTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "/*+ EXEC(database) */ X := SELECT e.* FROM Orders.entries e;");

//    assertFalse(env.getOps().get(0).getStatement().getHints().isEmpty());
//    SqlHint hint = (SqlHint) env.getOps().get(0).getStatement().getHints().get().get(0);
//    assertFalse(hint.getOperandList().isEmpty());
  }

  @Test
  public void selectListHintTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT /*+ NOOP */ e.* FROM Orders.entries AS e;");

//    assertFalse(((LogicalProject) env.getOps().get(0).getRelNode()).getHints().isEmpty());
  }

  @Test
  public void pathTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries AS e JOIN e.parent p;");
  }

  @Test
  public void invalidFunctionDef() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.test := NO_FUNC(100);\n");
  }

  @Test
  @Disabled
  public void invalidParentTable() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "Products.orders := SELECT COUNT(1) FROM @ JOIN Orders.entries e ON e.productid = @.productid;\n");
  }

  @Test
  public void importAllTest() {
    validateScript("IMPORT ecommerce-data.*;");
  }

  @Test
  public void importAllWithAliasTest() {
    validateScriptInvalid("IMPORT ecommerce-data.* AS ecommerce;");
  }

  @Test
  public void importAllWithTimestampTest() {
    validateScriptInvalid("IMPORT ecommerce-data.* TIMESTAMP _ingest_time AS c_ts;");
  }

  @Test
  public void importWithTimestamp() {
    validateScript("IMPORT ecommerce-data.Customer TIMESTAMP _ingest_time AS c_ts;");
    SQRLTable sqrlTable = (SQRLTable) planner.getSchema().getTable("Customer", false).getTable();
    assertTrue(sqrlTable.getField(Name.system("c_ts")).isPresent(), "Timestamp column missing");
  }

  @Test
  public void invalidImportDuplicateAliasTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Customer AS Product;\n");
  }

  @Test
  public void expressionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.descriptionLength := CHAR_LENGTH(description);");
  }

  @Test
  public void shadowExpressionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.descriptionLength := CHAR_LENGTH(description);"
        + "Product.descriptionLength := CHAR_LENGTH(description);");
  }

  @Test
  public void selectStarQueryTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "ProductCopy := SELECT * FROM Product;");
  }

  @Test
  public void coalesceTest() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "Orders.entries.discount2 := discount ? 0.0;");
  }

  @Test
  public void nestedCrossJoinQueryTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := SELECT * FROM @ JOIN Product;");
  }

  @Test
  public void nestedSelfJoinQueryTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productSelf := SELECT * FROM @;");
  }

  @Test
  public void invalidRootExpressionTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "ProductCount := count(Product);");
  }

  @Test
  public void invalidShadowRelationshipTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := JOIN Product;"
            + "Product.productCopy := JOIN Product;");
  }

  @Test
  public void testJoinDeclaration() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid ORDER BY _ingest_time;\n"
            + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;");
  }

  @Test
  @Disabled
  public void testOrderedJoinDeclaration() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid;\n"
            + "Product.joinDeclaration := JOIN Product ON true ORDER BY Product.productid;");
  }

  @Test
  public void invalidJoinDeclarationOnRootTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product2 := JOIN Product ON @.productid = Product.productid;");
  }

  @Test
  public void joinDeclarationOnRootTet() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product2 := JOIN Product;");
  }

  @Test
  public void invalidExpressionAssignmentOnRelationshipTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.joinDeclaration.column := 1;");
  }

  @Test
  public void invalidQueryAssignmentOnRelationshipTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.joinDeclaration.column := SELECT * FROM @ JOIN Product;");
  }

  @Test
  public void invalidShadowJoinDeclarationTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;");
  }

  @Test
  public void tablePathTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "NewProduct := SELECT * FROM Product.joinDeclaration;");
  }

  @Test
  public void inlinePathTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON true;\n"
        + "NewProduct := SELECT joinDeclaration.productid FROM Product;");
  }

  @Test
  public void parentTest() {
    validateScript("IMPORT ecommerce-data.Orders; "
        + "IMPORT ecommerce-data.Product; \n"
        + "Product.orders_entries := JOIN Orders.entries e ON @.productid = e.productid;\n"
        + "NewProduct := SELECT j.parent.customerid FROM Product.orders_entries j;");
  }
//
//  @Test
//  public void joinDeclarationShadowTest() {
//    validateScript(
//        "IMPORT ecommerce-data.Product;\n" +
//            "PointsToProduct := SELECT * FROM Product ON @.productid = Product.productid;\n" +
//            "Product := SELECT 1 AS x, 2 AS y FROM Product;\n" +
//            "OldProduct := SELECT * FROM PointsToProduct;");
//
//    Optional<Table> pointsToProduct = generator.getSqrlSchema().getTable("PointsToProduct",
//    false)
//    //    Assertions.assertEquals(pointsToProduct.getFields().size(), 4);
//
//    Optional<Table> product = generator.getSqrlSchema().getTable("Product", false)
//    //    Assertions.assertEquals(pointsToProduct.getFields().size(), 2);
//
//    Optional<Table> oldProduct = generator.getSqrlSchema().getTable("OldProduct", false)
//    //    Assertions.assertEquals(pointsToProduct.getFields().size(), 4);
//  }

  @Test
  public void invalidRelationshipInColumnTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product2 := SELECT joinDeclaration FROM Product;");
  }

  @Test
  public void subQueryExpressionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product WHERE productid IN (SELECT productid FROM "
        + "Product);");
  }

  @Test
  public void crossJoinTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, Product.joinDeclaration;");
  }

  @Test
  public void subQueryTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, (SELECT MIN(productid) AS min FROM Product) f;");
  }

  @Test
  public void invalidSelfInSubqueryTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM @);");
  }

  @Test
  public void nestedUnionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.p2 := SELECT * FROM @\n"
        + "              UNION DISTINCT\n"
        + "              SELECT * FROM @;");
  }

  @Test
  public void unionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product UNION DISTINCT SELECT * FROM Product;");
  }

  @Test
  public void unionMixedTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT productid, name, category FROM Product\n"
        + "            UNION DISTINCT\n"
        + "            SELECT description, productid FROM Product;");
  }

  @Test
  public void unionAllTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product UNION ALL SELECT * FROM Product;");
  }

  @Test
  public void intervalTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 2 DAY AS x FROM Product;");
//    LogicalProject project = (LogicalProject) env.getOps().get(0).getRelNode();
//    RexCall call = (RexCall) project.getNamedProjects().get(0).left;
//    RexLiteral rexLiteral = (RexLiteral) call.getOperands().get(1);
//    assertTrue(rexLiteral.getValue() instanceof BigDecimal);
//    assertEquals(BigDecimal.valueOf(24), rexLiteral.getValue());
//    assertTrue(rexLiteral.getType() instanceof IntervalSqlType);
//    assertEquals(
//        TimeUnit.YEAR,
//        rexLiteral.getType().getIntervalQualifier().getUnit());
  }

  @Test
  public void intervalSecondTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 2 HOUR AS x FROM Product;");
//    LogicalProject project = (LogicalProject) env.getOps().get(0).getRelNode();
//    RexCall call = (RexCall) project.getNamedProjects().get(0).left;
//    RexLiteral rexLiteral = (RexLiteral) call.getOperands().get(1);
//    assertTrue(rexLiteral.getValue() instanceof BigDecimal);
//    assertEquals(BigDecimal.valueOf(7200000), rexLiteral.getValue());
//    assertTrue(rexLiteral.getType() instanceof IntervalSqlType);
//    assertEquals(TimeUnit.HOUR,
//        rexLiteral.getType().getIntervalQualifier().getUnit());
  }


  @Test
  public void intervalSecondTest2() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 60 SECOND AS x FROM Product;");
//    LogicalProject project = (LogicalProject) env.getOps().get(0).getRelNode();
//    RexCall call = (RexCall) project.getNamedProjects().get(0).left;
//    RexLiteral rexLiteral = (RexLiteral) call.getOperands().get(1);
//    assertTrue(rexLiteral.getValue() instanceof BigDecimal);
//    assertEquals(BigDecimal.valueOf(60000), rexLiteral.getValue());
//    assertTrue(rexLiteral.getType() instanceof IntervalSqlType);
//    assertEquals(TimeUnit.SECOND,
//        rexLiteral.getType().getIntervalQualifier().getUnit());
  }

  @Test
  public void distinctStarTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := SELECT DISTINCT * FROM Product;");
  }

  @Test
  public void distinctWithGroupNotInSelectTest() {
    //todo: Fringe case to guard against
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := "
            + "  SELECT DISTINCT count(1) "
            + "  FROM @ JOIN Product p "
            + "  GROUP BY p.category;");
  }

  @Test
  public void topNTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := "
            + "  SELECT p.* "
            + "  FROM @ JOIN Product p "
            + "  LIMIT 5;");

//    assertFalse(((LogicalProject) env1.getOps().get(0).getRelNode()).getHints().isEmpty());
//    assertEquals(1,
//        ((LogicalProject) env1.getOps().get(0).getRelNode()).getHints().get(0).listOptions.size());
  }

  @Test
  public void distinctSingleColumnTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT DISTINCT productid FROM Product;");
//    assertFalse(((LogicalProject) env1.getOps().get(0).getRelNode()).getHints().isEmpty());
  }

  @Test
  public void localAggregateExpressionTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SUM(Product.productid);");
  }

  @Test
  public void localAggregateRelativePathTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.total := SUM(joinDeclaration.productid);");
  }

  @Test
  public void compoundAggregateExpressionTest() {
    //not yet supported
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.total := SUM(@.productid) + SUM(@.productid);");
  }

  @Test
  public void queryAsExpressionTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SELECT SUM(x.productid) - 1 AS sum FROM @ AS x;");
  }

  @Test
  public void localAggregateInQueryTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.total := SELECT SUM(joinDeclaration.productid) AS totals FROM @;");
  }

  @Test
  public void localAggregateCountTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.total := COUNT(joinDeclaration);");
  }

  @Test
  public void compoundJoinDeclarations() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.joinDeclaration2 := JOIN @.joinDeclaration j ON @.productid = j.productid;\n");
  }

  @Test
  public void localAggregateCountStarTest() {
    //complex columns not yet supported
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.total := COUNT(*);");
  }

  @Test
  public void localAggregateCountStarTest2() {
    //complex columns not yet supported
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.total := COUNT();");
  }

  @Test
  public void localAggregateInAggregateTest() {
    //Not yet supported
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.total := SUM(COUNT(joinDeclaration));");
  }

  @Test
  public void parameterizedLocalAggregateTest() {
    //complex column not yet supported
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid LIMIT 1;\n"
        + "Product.total := COALESCE(joinDeclaration.productid, 1000);\n");
  }

  @Test
  public void invalidParameterizedLocalAggregateTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.total := MIN(joinDeclaration.productid, joinDeclaration.parent.productid);\n");
  }

  @Test
  public void invalidInlinePathMultiplicityTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.joinDeclaration := JOIN Product ON true;\n"
            + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n");
  }

  @Test
  public void inlinePathMultiplicityTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON true LIMIT 1;\n"
        + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n");
  }

  @Test
  @Disabled
  //Automatically determining the order by statement not yet supported
  public void distinctOnTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid;\n");
  }

  @Test
  public void distinctOnWithExpression2Test() {
    validateScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Product2 := DISTINCT Orders ON id ORDER BY time DESC;\n");
  }

  @Test
  public void distinctOnWithExpressionTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := DISTINCT Product ON productid / 10 ORDER BY _ingest_time DESC;\n");
  }

  @Test
  public void distinctOnWithExpressionAliasTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := DISTINCT Product ON Product.productid / 10 ORDER BY _ingest_time DESC;\n");
  }

  @Test
  public void nestedGroupByTest() {
    validateScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT e.discount, count(1) AS cnt "
            + "                    FROM @ JOIN @.entries e"
            + "                    GROUP BY e.discount;\n");
  }

  @Test
  public void nestedAggregateNoGroupTest() {
    validateScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT count(1) AS cnt "
            + "                    FROM @.entries e");
  }

  @Test
  public void countFncTest() {
    validateScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT discount, count(*) AS cnt "
            + "                    FROM @.entries "
            + "                    GROUP BY discount;\n");
  }

  @Test
  @Disabled
  public void nestedLocalDistinctTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := SELECT p.productid FROM Product p;"
            + "Product.nested := DISTINCT @ ON @.productid;");
  }

  @Test
  public void uniqueOrderByTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product ORDER BY productid / 10;");
  }

  @Test
  public void invalidOrderTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries AS e ORDER BY e.parent;");
  }

  @Test
  public void groupTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.parent._uuid AS gp, min(e.unit_price) AS min_price"
        + "     FROM Orders.entries AS e "
        + "     GROUP BY e.parent._uuid;");
  }

  @Test
  public void orderTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.ordered_entries := SELECT e.* FROM @ JOIN @.entries AS e ORDER BY @._uuid;");
  }

  @Test
  public void queryNotAsExpressionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.example := SELECT p.productid FROM @ JOIN Product p;\n");
  }

  @Test
  public void queryAsExpressionTest2() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT x.productid FROM @ AS x;\n");
  }

  @Test
  public void queryAsExpressionUnnamedTest3() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product.example := SELECT @.productid + 1 AS pid FROM @ INNER JOIN Product ON true;\n");
  }

  @Test
  public void queryAsExpressionSameNamedTest4() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT sum(x.productid) AS example FROM @ AS x HAVING example > 10;\n");
  }

  @Test
  public void starAliasPrefixTest() {
    validateScript("IMPORT ecommerce-data.Product;" +
        "X := SELECT j.* FROM Product j JOIN Product h ON true;");
  }

  @Test
  public void invalidAliasJoinOrder() {
    validateScriptInvalid("IMPORT ecommerce-data.Orders;"
        + "X := SELECT * From Orders o JOIN o;");
  }

  @Test
  public void intervalJoinTest() {
    validateScript(
        "IMPORT ecommerce-data.Orders;"
            + "IMPORT ecommerce-data.Product;"
            + "X := SELECT * "
            + "     FROM Product AS p "
            + "     INTERVAL JOIN Orders AS o ON o._ingest_time > p._ingest_time;");
  }

  @Test
  public void castTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT CAST(1 AS String) AS cast1 From Orders;"
        + "X := SELECT CAST(1 AS Boolean) AS cast2 From Orders;"
        + "X := SELECT CAST(1 AS Float) AS cast3 From Orders;"
        + "X := SELECT CAST(1 AS Integer) AS cast4 From Orders;"
        + "X := SELECT CAST(1 AS DateTime) AS cast5 From Orders;");
  }

  @Test
  public void castExpression() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.x := CAST(1 AS String);");
  }

  @Test
  public void aggregateIsToOne() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(1) AS num, SUM(e.discount) AS total FROM @ JOIN @.entries e;\n"
        + "X := SELECT o.id, o.customerid, o.stats.num FROM Orders o;");
  }

  @Test
  public void aggregateIsToOne2() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(e.unit_price) AS num, SUM(e.discount) AS total FROM @.entries e;\n"
        + "X := SELECT o.id, o.customerid, o.stats.num FROM Orders o;");
  }

  @Test
  public void streamTest() {
    validateScript("IMPORT ecommerce-data.Customer;"
        + "Y := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;"
        + "X := STREAM ON ADD AS SELECT * From Y;");

    assertNotNull(
        planner.getSchema().getTable("X", false));
  }

  @Test
  public void timestampTest() {
    validateScript("IMPORT ecommerce-data.Orders TIMESTAMP time;");
  }

  @Test
  public void timestampAliasTest() {
    validateScript("IMPORT ecommerce-data.Orders AS O TIMESTAMP time;");
  }
}
