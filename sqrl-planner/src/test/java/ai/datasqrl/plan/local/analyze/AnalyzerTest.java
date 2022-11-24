package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCode;
import ai.datasqrl.parse.ParsingException;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Resolve.StatementOp;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.SnapshotTest.Snapshot;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.*;

class AnalyzerTest extends AbstractLogicalSQRLIT {

  private TestDataset example = Retail.INSTANCE;
  private Snapshot snapshot;


  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
    this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
  }

  private Env generate(ScriptNode node) {
    Env env = resolve.planDag(session, node);
    snapshotEnv(env);
    return env;
  }

  private void snapshotEnv(Env env) {
    if (env.getOps().size() == 0) {
      return;
    }
      for (StatementOp op : env.getOps()) {
      snapshot.addContent(String.format("%s", op.getRelNode().explain()));
    }

    snapshot.createOrValidate();
  }

  private void generateInvalid(ScriptNode node) {
    assertThrows(Exception.class,
        ()->resolve.planDag(session, node),
        "Statement should throw exception"
        );
  }

  private void generateInvalid(ScriptNode node, ErrorCode expectedCode) {
    try {
      resolve.planDag(session, node);
      fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testCatchingCalciteErrorTest() {
    generateInvalid(parser.parse(""
        + "IMPORT ecommerce-data.Product;"
        //Expression 'productid' is not being grouped
        + "X := SELECT productid, SUM(productid) FROM Product GROUP BY name"),
        ErrorCode.GENERIC_ERROR);
  }

  // IMPORTS
  @Test
  public void import1() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"));
  }

  @Test
  public void import2() {
    generate(parser.parse("IMPORT ecommerce-data.*;"));
  }

  @Test
  public void import3() {
    generate(parser.parse("IMPORT ecommerce-data.Orders AS O;"));
  }

  @Test
  public void importInvalidPathTest() {
    generateInvalid(parser.parse("IMPORT Product;"));
  }

  @Test
  public void duplicateImportTest() {
    generateInvalid(
        parser.parse("IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Product;\n"), ErrorCode.IMPORT_NAMESPACE_CONFLICT);
  }

  @Test
  public void absoluteTest1() {
    generate(parser.parse("IMPORT ecommerce-data.Product;"
        + "X := SELECT productid FROM Product;"));
  }
  @Test
  public void absoluteTest2() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT discount FROM Orders.entries;"));
  }

  @Test
  public void relativeTest1() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.entries.d2 := SELECT _.discount FROM _;"));
  }

  @Test
  public void relativeTest2() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.x := SELECT discount FROM _.entries;"));
  }

  @Test
  public void noPathOrderByTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries e ORDER BY e.discount DESC;"));
  }

  @Test
  public void assignmentHintTest() {
    Env env = generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "/*+ EXEC(database) */ X := SELECT e.* FROM Orders.entries e;"));

    assertFalse(env.getOps().get(0).getStatement().getHints().isEmpty());
    SqlHint hint = (SqlHint)env.getOps().get(0).getStatement().getHints().get().get(0);
    assertFalse(hint.getOperandList().isEmpty());
  }

  @Test
  public void selectListHintTest() {
    Env env = generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT /*+ NOOP */ e.* FROM Orders.entries AS e;"));

    assertFalse(((LogicalProject) env.getOps().get(0).getRelNode()).getHints().isEmpty());
  }

  @Test
  public void pathTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries AS e JOIN e.parent p;"));
  }

  @Test
  public void invalidFunctionDef() {
    generateInvalid(
        parser.parse("IMPORT ecommerce-data.Product;\n"
            + "Product.test := NOW(100);\n"));
  }

  @Test
  public void importAllTest() {
    generate(parser.parse("IMPORT ecommerce-data.*;"));
  }

  @Test
  public void importAllWithAliasTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.* AS ecommerce;"),
        ErrorCode.IMPORT_CANNOT_BE_ALIASED);
  }

  @Test
  public void importAllWithTimestampTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.* TIMESTAMP _ingest_time AS c_ts;"),
        ErrorCode.IMPORT_STAR_CANNOT_HAVE_TIMESTAMP);
  }

  @Test
  public void importWithTimestamp() {
    Env env1 = generate(parser.parse("IMPORT ecommerce-data.Customer TIMESTAMP _ingest_time AS c_ts;"));
    SQRLTable sqrlTable = (SQRLTable) env1.getRelSchema().getTable("Customer", false).getTable();
    assertTrue(sqrlTable.getField(Name.system("c_ts")).isPresent(), "Timestamp column missing");
  }

  @Test
  public void invalidImportDuplicateAliasTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Customer AS Product;\n"));
  }

  @Test
  public void expressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.descriptionLength := CHAR_LENGTH(description);"));
  }

  @Test
  public void shadowExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.descriptionLength := CHAR_LENGTH(description);"
        + "Product.descriptionLength := CHAR_LENGTH(description);"));
  }

  @Test
  public void selectStarQueryTest() {
    generate(
        parser.parse("IMPORT ecommerce-data.Product;\n"
            + "ProductCopy := SELECT * FROM Product;"));
  }

  @Test
  public void coalesceTest() {
    generate(
        parser.parse("IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries.discount2 := discount ? 0.0;"));
  }

  @Test
  public void nestedCrossJoinQueryTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := SELECT * FROM _ JOIN Product;"));
  }

  @Test
  public void nestedSelfJoinQueryTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productSelf := SELECT * FROM _;"));
  }

  @Test
  public void invalidRootExpressionTest() {
    generateInvalid(
        parser.parse("IMPORT ecommerce-data.Product;\n"
            + "ProductCount := count(Product);"));
  }

  @Test
  public void invalidShadowRelationshipTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := JOIN Product;"
            + "Product.productCopy := JOIN Product;"));
  }

  @Test
  public void testJoinDeclaration() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid ORDER BY _ingest_time;\n"
            + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;"));
  }

  @Test
  @Disabled
  public void testOrderedJoinDeclaration() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid;\n"
            + "Product.joinDeclaration := JOIN Product ON true ORDER BY Product.productid;"));
  }

  @Test
  public void invalidJoinDeclarationOnRootTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := JOIN Product ON _.productid = Product.productid;"));
  }

  @Test
  public void joinDeclarationOnRootTet() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := JOIN Product;"));
  }

  @Test
  public void invalidExpressionAssignmentOnRelationshipTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.joinDeclaration.column := 1;"));
  }

  @Test
  public void invalidQueryAssignmentOnRelationshipTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.joinDeclaration.column := SELECT * FROM _ JOIN Product;"));
  }

  @Test
  public void invalidShadowJoinDeclarationTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;"));
  }

  @Test
  public void tablePathTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "NewProduct := SELECT * FROM Product.joinDeclaration;"));
  }

  @Test
  public void inlinePathTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON true;\n"
        + "NewProduct := SELECT joinDeclaration.productid FROM Product;"));
  }

  @Test
  public void parentTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders; "
        + "IMPORT ecommerce-data.Product; \n"
        + "Product.orders_entries := JOIN Orders.entries e ON _.productid = e.productid;\n"
        + "NewProduct := SELECT j.parent.customerid FROM Product.orders_entries j;"));
  }
//
//  @Test
//  public void joinDeclarationShadowTest() {
//    generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "PointsToProduct := SELECT * FROM Product ON _.productid = Product.productid;\n" +
//            "Product := SELECT 1 AS x, 2 AS y FROM Product;\n" +
//            "OldProduct := SELECT * FROM PointsToProduct;"));
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
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT joinDeclaration FROM Product;"));
  }

  @Test
  public void subQueryExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product WHERE productid IN (SELECT productid FROM "
        + "Product);"));
  }

  @Test
  public void crossJoinTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, Product.joinDeclaration;"
    ));
  }

  @Test
  public void subQueryTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM Product) f;"));
  }

  @Test
  public void invalidSelfInSubqueryTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM _);"));
  }

  @Test
  public void nestedUnionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.p2 := SELECT * FROM _\n"
        + "              UNION DISTINCT\n"
        + "              SELECT * FROM _;"));
  }

  @Test
  public void unionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product UNION DISTINCT SELECT * FROM Product;"));
  }

  @Test
  public void unionMixedTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT productid, name, category FROM Product\n"
        + "            UNION DISTINCT\n"
        + "            SELECT description, productid FROM Product;"));
  }

  @Test
  public void unionAllTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product UNION ALL SELECT * FROM Product;"));
  }

  @Test
  public void intervalTest() {
    Env env = generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 2 YEAR AS x FROM Product;"));
    LogicalProject project = (LogicalProject)env.getOps().get(0).getRelNode();
    RexCall call = (RexCall)project.getNamedProjects().get(0).left;
    RexLiteral rexLiteral = (RexLiteral)call.getOperands().get(1);
    assertTrue(rexLiteral.getValue() instanceof BigDecimal);
    assertEquals(BigDecimal.valueOf(24), rexLiteral.getValue());
    assertTrue(rexLiteral.getType() instanceof IntervalSqlType);
    assertEquals(
        TimeUnit.YEAR,
        rexLiteral.getType().getIntervalQualifier().getUnit());
  }

  @Test
  public void intervalSecondTest() {
    Env env = generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 2 HOUR AS x FROM Product;"));
    LogicalProject project = (LogicalProject)env.getOps().get(0).getRelNode();
    RexCall call = (RexCall)project.getNamedProjects().get(0).left;
    RexLiteral rexLiteral = (RexLiteral)call.getOperands().get(1);
    assertTrue(rexLiteral.getValue() instanceof BigDecimal);
    assertEquals(BigDecimal.valueOf(7200000), rexLiteral.getValue());
    assertTrue(rexLiteral.getType() instanceof IntervalSqlType);
    assertEquals(TimeUnit.HOUR,
        rexLiteral.getType().getIntervalQualifier().getUnit());
  }


  @Test
  public void intervalSecondTest2() {
    Env env = generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 60 SECOND AS x FROM Product;"));
    LogicalProject project = (LogicalProject)env.getOps().get(0).getRelNode();
    RexCall call = (RexCall)project.getNamedProjects().get(0).left;
    RexLiteral rexLiteral = (RexLiteral)call.getOperands().get(1);
    assertTrue(rexLiteral.getValue() instanceof BigDecimal);
    assertEquals(BigDecimal.valueOf(60000), rexLiteral.getValue());
    assertTrue(rexLiteral.getType() instanceof IntervalSqlType);
    assertEquals(TimeUnit.SECOND,
        rexLiteral.getType().getIntervalQualifier().getUnit());
  }

  @Test
  public void distinctStarTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := SELECT DISTINCT * FROM Product;"));
  }

  @Test
  public void distinctWithGroupNotInSelectTest() {
    //todo: Fringe case to guard against
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := "
            + "  SELECT DISTINCT count(1) "
            + "  FROM _ JOIN Product p "
            + "  GROUP BY p.category;"));
  }

  @Test
  public void topNTest() {
    Env env1 = generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := "
            + "  SELECT p.* "
            + "  FROM _ JOIN Product p "
            + "  LIMIT 5;"));

    assertFalse(((LogicalProject) env1.getOps().get(0).getRelNode()).getHints().isEmpty());
    assertEquals(1, ((LogicalProject) env1.getOps().get(0).getRelNode()).getHints().get(0).listOptions.size());
  }

  @Test
  public void distinctSingleColumnTest() {
    Env env1 = generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT DISTINCT productid FROM Product;"));
    assertFalse(((LogicalProject) env1.getOps().get(0).getRelNode()).getHints().isEmpty());
  }

  @Test
  public void localAggregateExpressionTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SUM(Product.productid);"));
  }

  @Test
  public void localAggregateRelativePathTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SUM(joinDeclaration.productid);"));
  }

  @Test
  public void compoundAggregateExpressionTest() {
    //not yet supported
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := SUM(_.productid) + SUM(_.productid);"));
  }

  @Test
  public void queryAsExpressionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SELECT SUM(x.productid) - 1 FROM _ AS x;"));
  }

  @Test
  public void localAggregateInQueryTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SELECT SUM(joinDeclaration.productid) AS totals FROM _;"));
  }

  @Test
  public void localAggregateCountTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := COUNT(joinDeclaration);"));
  }

  @Test
  public void compoundJoinDeclarations() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.joinDeclaration2 := JOIN _.joinDeclaration j ON _.productid = j.productid;\n"));
  }

  @Test
  public void localAggregateCountStarTest() {
    //complex columns not yet supported
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := COUNT(*);"));
  }

  @Test
  public void localAggregateCountStarTest2() {
    //complex columns not yet supported
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := COUNT();"));
  }

  @Test
  public void localAggregateInAggregateTest() {
    //Not yet supported
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SUM(COUNT(joinDeclaration));"));
  }

  @Test
  public void parameterizedLocalAggregateTest() {
    //complex column not yet supported
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid LIMIT 1;\n"
        + "Product.total := COALESCE(joinDeclaration.productid, 1000);\n"));
  }

  @Test
  public void invalidParameterizedLocalAggregateTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := MIN(joinDeclaration.productid, joinDeclaration.parent.productid);\n"));
  }

  @Test
  public void invalidInlinePathMultiplicityTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.joinDeclaration := JOIN Product ON true;\n"
            + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
  }

  @Test
  public void inlinePathMultiplicityTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON true LIMIT 1;\n"
        + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
  }

  @Test
  public void distinctOnTest() {
    //Automatically determining the order by statement not yet supported
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid;\n"));
  }

  @Test
  public void distinctOnWithExpression2Test() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;\n"
            + "Product2 := DISTINCT Orders ON id ORDER BY time DESC;\n"));
  }

  @Test
  public void distinctOnWithExpressionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := DISTINCT Product ON productid / 10 ORDER BY _ingest_time DESC;\n"));
  }

  @Test
  public void distinctOnWithExpressionAliasTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := DISTINCT Product ON Product.productid / 10 ORDER BY _ingest_time DESC;\n"));
  }

  @Test
  public void nestedGroupByTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT e.discount, count(1) AS cnt "
            + "                    FROM _ JOIN _.entries e"
            + "                    GROUP BY e.discount;\n"));
  }

  @Test
  public void nestedAggregateNoGroupTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT count(1) AS cnt "
            + "                    FROM _.entries e"));
  }

  @Test
  public void countFncTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries_2 := SELECT discount, count(*) AS cnt "
            + "                    FROM _.entries "
            + "                    GROUP BY discount;\n"));
  }

  @Test
  public void nestedLocalDistinctTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := SELECT p.productid FROM Product p;"
            + "Product.nested := DISTINCT _ ON _.productid;"));
  }

  @Test
  @Ignore
  public void invalidNestedDistinctTest() {
    assertThrows(ParsingException.class,
        ()->parser.parse(
            "IMPORT ecommerce-data.Product;\n"
                + "Product.nested := SELECT productid FROM Product;\n"
                + "Product2 := DISTINCT Product.nested ON productid;"
        ),
        "Path not nestable");
  }

  @Test
  public void uniqueOrderByTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product ORDER BY productid / 10;"));
  }

  @Test
  public void invalidOrderTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries AS e ORDER BY e.parent;"));
  }

  @Test
  public void invalidGroupTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.parent._uuid AS gp, min(e.unit_price) "
        + "     FROM Orders.entries AS e "
        + "     GROUP BY e.parent._uuid;"));
  }

  @Test
  public void orderTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.ordered_entries := SELECT e.* FROM _ JOIN _.entries AS e ORDER BY _._uuid;"));
  }

  @Test
  public void queryNotAsExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.example := SELECT p.productid FROM _ JOIN Product p;\n"));
  }

  @Test
  public void queryAsExpressionTest2() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT x.productid FROM _ AS x;\n"));
  }

  @Test
  public void queryAsExpressionUnnamedTest3() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.example := SELECT _.productid + 1 FROM _ INNER JOIN Product ON true;\n"));
  }

  @Test
  public void queryAsExpressionSameNamedTest4() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT sum(x.productid) AS example FROM _ AS x HAVING example > 10;\n"));
  }

  @Test
  public void starAliasPrefixTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;" +
        "X := SELECT j.* FROM Product j JOIN Product h ON true;"));
  }

  @Test
  public void invalidDistinctOrder() {
  }

  @Test
  public void invalidAliasJoinOrder() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT * From Orders o JOIN o;"));
  }

  @Test
  public void intervalJoinTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;"
            + "IMPORT ecommerce-data.Product;"
            + "X := SELECT * "
            + "     FROM Product AS p "
            + "     INTERVAL JOIN Orders AS o ON o._ingest_time > p._ingest_time;"
    ));
  }

  @Test
  public void castTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT CAST(1 AS String) From Orders;"
        + "X := SELECT CAST(1 AS Boolean) From Orders;"
        + "X := SELECT CAST(1 AS Float) From Orders;"
        + "X := SELECT CAST(1 AS Integer) From Orders;"
        + "X := SELECT CAST(1 AS DateTime) From Orders;"
    ));
  }

  @Test
  public void castExpression() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.x := CAST(1 AS String);"));
  }

  @Test
  public void aggregateIsToOne() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(1) AS num, SUM(e.discount) AS total FROM _ JOIN _.entries e;\n"
        + "X := SELECT o.id, o.customerid, o.stats.num FROM Orders o;"
    ));
  }

  @Test
  public void aggregateIsToOne2() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(e.unit_price) AS num, SUM(e.discount) AS total FROM _.entries e;\n"
        + "X := SELECT o.id, o.customerid, o.stats.num FROM Orders o;"));
  }

  @Test
  public void streamTest() {
    Env env1 = generate(parser.parse("IMPORT ecommerce-data.Customer;"
            + "Y := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;"
        + "X := STREAM ON ADD AS SELECT * From Y;"));

    assertNotNull(
        env1.getRelSchema().getTable("X", false));
  }

  @Test
  public void timestampTest() {
    Env env1 = generate(parser.parse("IMPORT ecommerce-data.Orders TIMESTAMP time;"));
  }
}
