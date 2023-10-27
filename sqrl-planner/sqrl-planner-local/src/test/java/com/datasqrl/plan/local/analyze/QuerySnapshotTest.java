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
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.rules.IdealExecutionStage;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
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
      snapshot.createOrValidate();

    } catch (Exception e) {
      e.printStackTrace();
      fail("Unknown exception", e);
    }
  }

  protected void validateScript(String script) {
    Namespace ns = plan(script);
    SQRLConverter sqrlConverter = new SQRLConverter(planner.createRelBuilder());
    Stream.concat(ns.getSchema().getFunctionStream(QueryTableFunction.class).map(QueryTableFunction::getQueryTable),
        ns.getSchema().getTableStream(PhysicalRelationalTable.class))
        .sorted(Comparator.comparing(f->f.getNameId()))
        .forEach(table-> {
          SQRLConverter.Config config = table.getBaseConfig().stage(IdealExecutionStage.INSTANCE).build();
          snapshot.addContent(
              sqrlConverter.convert(table, config, false, errors).explain(),
              table.getNameId());
        });
    if (!errors.isEmpty()) {
      snapshot.addContent(ErrorPrinter.prettyPrint(errors), "warnings");
    }
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
  public void accessTableFunctionTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X(@id: BigInt) := SELECT * FROM Customer WHERE customerid = @id");
    validateScript(builder.getScript());
  }

  @Test
  public void computeTableFunctionTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("X(@id: bigint) := SELECT *, 1 AS x FROM Customer WHERE customerid = @id");
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
    builder.add("Orders.o2 := SELECT @.* FROM @ JOIN Orders");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentIdTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("D := SELECT p.id FROM Orders.entries AS e JOIN e.parent p");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentCustomerTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("D := SELECT * FROM Orders.entries e INNER JOIN e.parent p WHERE p.customerid = 0");
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
    builder.add("Orders.entries.x := SELECT p.id, @.discount FROM @ JOIN @.parent p");
    validateScript(builder.getScript());
  }

  @Test
  public void orderParentIdDiscountConditionTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.x := SELECT p.id, x.discount FROM @ AS x JOIN x.parent p WHERE p.id = 1");
    validateScript(builder.getScript());
  }

  @Test
  public void customerDistinctTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC");
    validateScript(builder.getScript());
  }

  @Test
  public void fromTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("CustomerById(@id: INT) := FROM Customer WHERE customerid = @id;");
    validateScript(builder.getScript());
  }
  @Test
  public void fromInvalidTableTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("CustomerById(@id: INT) := FROM x WHERE customerid = @id;");
    validateScriptInvalid(builder.getScript());
  }
  @Test
  public void fromMultilineInvalidTableTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("CustomerById(@id: INT) := FROM \n x WHERE customerid = @id;");
    validateScriptInvalid(builder.getScript());
  }

  @Test
  public void invalidMultilineQueryTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer := SELECT * FROM Customer\nWHERE x = null;");
    validateScriptInvalid(builder.getScript());
  }

  @Test
  public void invalidDistinctSelectTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer := DISTINCT Customer ON x ORDER BY _ingest_time DESC");
    validateScriptInvalid(builder.getScript());
  }

  @Test
  public void invalidDistinctTableTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer := DISTINCT x ON customerid ORDER BY _ingest_time DESC");
    validateScriptInvalid(builder.getScript());
  }

  @Test
  public void invalidDistinctOrderTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY x DESC");
    validateScriptInvalid(builder.getScript());
  }

  @Test
  public void orderCoalesceTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.discount0 := SELECT coalesce(x.discount, 0.0) AS discount FROM @ AS x");
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
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid;");
    builder.add("Customer.recent_products := SELECT e.productid, coalesce(pp.category,'') AS category,\n"
        + "                                       sum(e.quantity) AS quantity, count(1) AS num_orders\n"
        + "                                FROM @.orders.entries AS e LEFT JOIN e.parent p LEFT JOIN e.product pp\n"
        + "                                WHERE p.time > now() - INTERVAL 365 DAYS\n"
        + "                                GROUP BY productid, category ORDER BY count(1) DESC, quantity DESC;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void orders2Test() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders3 := SELECT * FROM Orders.entries.parent.entries p;\n"
        + "Orders.biggestDiscount := JOIN @.entries e ORDER BY e.discount DESC;\n"
        + "Orders2 := SELECT * FROM Orders.biggestDiscount.parent e;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersEntriesTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries2 := SELECT @.id, @.time FROM @ JOIN @.entries;\n");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersEntriesDiscountTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.entries.discount0 := COALESCE(discount, 0.0);\n"
        + "Orders.entries.total := quantity * unit_price - discount0;");
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
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid;\n"
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
    builder.add("Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n");
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid");
    builder.add("Orders.entries.dProduct := SELECT unit_price, p.category, p.name FROM @ LEFT JOIN @.product p");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersNewIdTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.newid := SELECT NOW(), ParseTimestamp(TimestampToString(EpochToTimestamp(100))) FROM @ JOIN Orders;");
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
  public void caseWhenTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("CustomerNames := SELECT *\n"
        + "FROM Orders.entries\n"
        + "ORDER BY (CASE\n"
        + "    WHEN discount IS NULL THEN 0\n"
        + "    ELSE discount\n"
        + "END);");
    validateScript(builder.getScript());
  }

  @Test
  public void ordersX2Test() {
    ScriptBuilder builder = example.getImports();
    builder.add("Orders.x := SELECT x.* FROM @ JOIN @ AS x");
    //Invalid, cannot walk this
    validateScriptInvalid(builder.getScript());
  }

  @Test
  public void testCatchingCalciteErrorTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
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
    validateScript("IMPORT ecommerce-data.Product;\n"
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
  public void invalidParentTable() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "Products.orders := SELECT COUNT(1) FROM @ JOIN Orders.entries e ON e.productid = @.productid;\n");
  }

  @Test
  public void importAllTest() {
    validateScript("IMPORT ecommerce-data.*;");
  }

  @Test //todo: issue warning or just fail?
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
    RelOptTable table = framework.getCatalogReader().getTableFromPath(List.of("Customer"));
    int cTs = framework.getCatalogReader().nameMatcher()
        .indexOf(table.getRowType().getFieldNames(), "c_ts");
    assertTrue(cTs != -1, "Timestamp column missing");
  }

  @Test
  public void importWithTimestampAndAlias() {
    validateScript("IMPORT ecommerce-data.Customer AS C2 TIMESTAMP _ingest_time AS c_ts;");
    RelOptTable table = framework.getCatalogReader().getTableFromPath(List.of("C2"));
    int cTs = framework.getCatalogReader().nameMatcher()
        .indexOf(table.getRowType().getFieldNames(), "c_ts");
    assertTrue(cTs != -1, "Timestamp column missing");
  }

  @Test
  public void importDuplicateAliasTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Customer AS Product;\n");
  }

  @Test
  public void expressionTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
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
        + "Orders.entries.discount2 := COALESCE(discount,0.0);");
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
  public void duplicateParamTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product(@id: Int) := SELECT * FROM Product WHERE @id > productid AND @id < productid;");
  }

  @Test
  public void replaceRelationshipTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := JOIN Product;"
            + "Product.productCopy := JOIN Product;");
  }

  @Test
  public void testJoinDeclaration() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
            + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;");
  }

  @Test
  public void testOrderedJoinDeclaration() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
            + "Product.joinDeclaration := JOIN Product ON true ORDER BY Product.productid;");
  }

  @Test
  public void invalidJoinDeclarationOnRootTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product2 := JOIN Product ON @.productid = Product.productid;");
  }

  @Test
  public void joinDeclarationOnRootTest() {
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
  public void replaceJoinDeclarationTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
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
        + "NewProduct := SELECT p.customerid FROM Product.orders_entries j LEFT JOIN j.parent p;");
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
  public void invalidTableLockedTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Customer;\n"
            + "Customer2 := SELECT * FROM Customer;\n"
            + "Customer.column := 1");
  }

  @Test
  public void validateMultiplePKWarning() {
    validateScript("IMPORT ecommerce-data.Customer;\n"
            + "Customer2 := SELECT _uuid as id1, _uuid as id2, customerid + 5 as newid FROM Customer;");
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
        + "Product.table := SELECT * FROM @, (SELECT MIN(productid) FROM @.parent);");
  }

  @Test
  public void nestedUnionTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.nested := SELECT * FROM Product\n"
        + "              UNION ALL\n"
        + "              SELECT * FROM Product;");
  }

  @Test
  public void unionTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product UNION DISTINCT SELECT * FROM Product;");
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
  @Disabled
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
  }
  @Test
  public void shadowUuidTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := SELECT p.* FROM @ JOIN Product p;\n"
            + "X := SELECT _uuid FROM Product.nested;");
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
            + "Product.total := SUM(productid);");
  }

  @Test
  public void localAggregateRelativePathTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
        + "Product.total := SUM(joinDeclaration.productid);");
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
  public void parameterizedLocalAggregateTest() {
    //complex column not yet supported
    validateScriptInvalid("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON @.productid = Product.productid;\n"
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
        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
        + "Product.joinDeclaration := JOIN Product p ON @.productid = p.productid;\n"
        + "Product2 := SELECT j.productid, p.productid FROM Product p LEFT JOIN p.joinDeclaration j;\n");
  }

  @Test
  public void leftJoinWithoutCoalesce() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o LEFT JOIN Customer c ON o.customerid=c.customerid;");
  }

  @Test
  public void leftRightWithoutCoalesce() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o RIGHT JOIN Customer c ON o.customerid=c.customerid;");
  }

  @Test
  public void normalJoins() {
    validateScript("IMPORT ecommerce-data.*;\n"
        + "CustomerOrders1 := SELECT o.id, c.name FROM Orders o INNER JOIN Customer c ON o.customerid=c.customerid;\n"
        + "CustomerOrders2 := SELECT coalesce(c._uuid, '') as cuuid, o.id, c.name FROM Orders o LEFT JOIN Customer c ON o.customerid=c.customerid;\n"
        + "CustomerOrders3 := SELECT coalesce(o._uuid, '') as ouuid, o.id, c.name FROM Orders o RIGHT JOIN Customer c ON o.customerid=c.customerid;\n"
    );
  }

  @Test
  public void intervalJoins() {
    validateScript("IMPORT ecommerce-data.*;\n"
        + "CustomerOrders1 := SELECT o.id, c.name FROM Orders o INTERVAL JOIN Customer c ON o._ingest_time < c._ingest_time;\n"
        + "CustomerOrders2 := SELECT coalesce(c._uuid, '') as cuuid, o.id, c.name FROM Orders o LEFT INTERVAL JOIN Customer c ON o._ingest_time < c._ingest_time;\n"
        + "CustomerOrders1 := SELECT coalesce(o._uuid, '') as ouuid, o.id, c.name FROM Orders o RIGHT INTERVAL JOIN Customer c ON o._ingest_time < c._ingest_time;\n"
    );
  }


  @Test
  public void intervalJoinWithoutTimeBound() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o INTERVAL JOIN Customer c ON o.customerid=c.customerid;");
  }

  @Test
  public void intervalJoinOnState() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o INTERVAL JOIN Customer c ON o.customerid=c.customerid;");
  }

  @Test
  public void temporalJoins() {
    validateScript("IMPORT ecommerce-data.*;\n"
        + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o TEMPORAL JOIN Customer c ON o.customerid=c.customerid;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o LEFT TEMPORAL JOIN Customer c ON o.customerid=c.customerid;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Customer c RIGHT TEMPORAL JOIN Orders o ON o.customerid=c.customerid;");
  }

  @Test
  public void temporalJoinOnStreams() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o TEMPORAL JOIN Customer c ON o.customerid=c.customerid;");
  }

  @Test
  public void temporalJoinNotPKConstrained() {
    validateScriptInvalid("IMPORT ecommerce-data.*;\n"
        + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
        + "CustomerOrders := SELECT o.id, c.name FROM Orders o TEMPORAL JOIN Customer c ON o.customerid > c.customerid;");
  }

  @Test
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
            + "Product2 := DISTINCT Product ON productid / 10 AS pid ORDER BY _ingest_time DESC;\n");
  }

  @Test
  public void distinctOnWithExpressionAliasTest() {
    validateScript(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := DISTINCT Product ON Product.productid / 10 AS pid ORDER BY _ingest_time DESC;\n");
  }

  @Test
  public void unnamedColumn() {
    validateScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.unnamed := SELECT coalesce(customerid,0) FROM @;\n");
  }

  @Test
  public void nestedGroupByTest() {
    validateScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT coalesce(discount,0) AS discount, count(1) AS cnt "
            + "                    FROM @ JOIN @.entries e"
            + "                    GROUP BY discount;\n");
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
            + "Orders.entries_2 := SELECT coalesce(discount,0) AS discount, count(*) AS cnt "
            + "                    FROM @.entries "
            + "                    GROUP BY discount;\n");
  }

  @Test
  public void nestedLocalDistinctTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := DISTINCT @ ON @.productid ORDER BY _ingest_time DESC;");
  }

  /**
   * Nested queries must start with a @
   */
  @Test
  public void invalidNestedQueryTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := SELECT p.productid FROM Product p;");
  }
  @Test
  public void invalidNestedSubqueryQueryTest() {
    validateScriptInvalid(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := SELECT p.productid FROM (SELECT * FROM Product) p;");
  }

  @Test
  public void uniqueOrderByTest() {
    validateScript("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product ORDER BY productid / 10;");
  }

  @Test
  public void invalidOrderTest() {
    validateScriptInvalid("IMPORT ecommerce-data.Orders;\n"
        + "X := SELECT e.* FROM Orders.entries AS e ORDER BY e.parent;");
  }

  @Test
  public void groupTest() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "X := SELECT p._uuid AS gp, min(e.unit_price) AS min_price"
        + "     FROM Orders.entries AS e JOIN e.parent p"
        + "     GROUP BY p._uuid;");
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
    validateScriptInvalid("IMPORT ecommerce-data.Orders;\n"
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
        + "X := SELECT CAST(1 AS Double) AS cast3 From Orders;"
        + "X := SELECT CAST(1 AS Int) AS cast4 From Orders;"
        + "X := SELECT CAST(1 AS Timestamp) AS cast5 From Orders;");
  }

  @Test
  public void testStrToMap() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "IMPORT string.*;\n"
        + "Orders.map := strToMap('x=y')");

  }
  @Test
  public void testMapWithKey() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "IMPORT string.*;\n"
        + "Orders.map := strToMap('x=y')['x']");
  }

  @Test
  public void testUtf8() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "IMPORT string.*;\n"
        + "Orders.map := '🎶'");
  }

  @Test
  public void testTimeLiteral() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "Order_time := SELECT time, TIME '20:17:40' AS time FROM Orders;\n");
  }

  @Test
  public void testHourLiteral() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "Order_time := SELECT time AS hour, EXTRACT(HOUR FROM NOW()) AS hour FROM Orders;\n");
  }
  @Test
  public void testWeekLiteral() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "Order_time := SELECT time AS week, EXTRACT(WEEK FROM NOW()) AS week FROM Orders;\n");
  }
  @Test
  public void testCountAsColumnName() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "Order_time := SELECT COUNT(*) AS count FROM Orders;\n");
  }

  @Test
  public void testMonthLiteral() {
    validateScript("IMPORT ecommerce-data.Orders;\n"
        + "Order_time := SELECT time AS month, EXTRACT(MONTH FROM NOW()) AS month FROM Orders;\n");
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
        + "X := SELECT o.id, o.customerid, s.num FROM Orders o LEFT JOIN o.stats s;");
  }

  @Test
  public void aggregateIsToOne2() {
    validateScript("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(e.unit_price) AS num, SUM(e.discount) AS total FROM @.entries e;\n"
        + "X := SELECT o.id, o.customerid, s.num FROM Orders o LEFT JOIN o.stats s;");
  }

  @Test
  public void streamTest() {
    validateScript("IMPORT ecommerce-data.Customer;"
        + "Y := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;"
        + "X := STREAM ON ADD AS SELECT * From Y;");

    assertNotNull(this.framework.getCatalogReader().getTableFromPath(List.of("X")));
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
