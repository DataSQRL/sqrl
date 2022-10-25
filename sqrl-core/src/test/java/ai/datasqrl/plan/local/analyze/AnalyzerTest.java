package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.errors.ErrorCode;
import ai.datasqrl.errors.SqrlException;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.ParsingException;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.util.data.C360;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static ai.datasqrl.util.data.C360.RETAIL_DIR_BASE;
import static org.junit.jupiter.api.Assertions.*;

class AnalyzerTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;

  ErrorCollector error;

  private Session session;
  private Resolve resolve;

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
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());

    Planner planner = new PlannerFactory(
        schema.plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve(RETAIL_DIR_BASE.resolve("build"));
  }

  private Env generate(ScriptNode node) {
    return resolve.planDag(session, node);
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
      //todo: unified error handling
//      e.printStackTrace();
//      assertTrue(e instanceof SqrlException, "Should be SqrlException is: " + e.getClass().getName());
//      SqrlException exception = (SqrlException) e;
//      assertEquals(expectedCode, exception.getErrorCode());
//      System.out.println(exception.getMessage());
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
  @Disabled
  public void functionImportTest() {
    generate(parser.parse(""
        + "IMPORT system.functions;"
        + "IMPORT ecommerce-data.Orders;"));
  }

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
  @Disabled
  public void noPathOrderByTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries e ORDER BY e.discount DESC;"));
  }

  @Test
  public void assignmentHintTest() {
    Env env = generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "/*+ NOOP */ X := SELECT e.* FROM Orders.entries e;"));

    assertFalse(env.getOps().get(0).getStatement().getHints().isEmpty());
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
  @Disabled
  public void invalidFunctionDef() {
    generateInvalid(
        parser.parse("IMPORT ecommerce-data.Product;\n"
            + "Product.test := NOW(100);\n"));
  }

  @Test
  @Disabled
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
  @Disabled
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
  @Disabled
  public void invalidShadowRelationshipTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := JOIN Product;"
            + "Product.productCopy := JOIN Product;"));
  }

  @Test
  @Disabled
  public void testJoinDeclaration() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product := DISTINCT Product ON productid;\n"
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
  @Disabled
  public void joinDeclarationOnRootTet() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := JOIN Product;"));
  }

  @Test
  @Disabled
  public void invalidExpressionAssignmentOnRelationshipTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.joinDeclaration.column := 1;"));
  }

  @Test
  public void invalidQueryAssignmentOnRelationshipTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.joinDeclaration.column := SELECT * FROM _ JOIN Product;"));
  }

  @Test
  @Disabled
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
  @Disabled
  public void inlinePathTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON true;\n"
        + "NewProduct := SELECT joinDeclaration.productid FROM Product;"));
  }

  @Test
  @Disabled
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
  @Disabled
  public void subQueryExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product WHERE productid IN (SELECT productid FROM "
        + "Product);"));
  }

  @Test
  @Disabled
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
  @Disabled
  public void unionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT * FROM Product UNION DISTINCT SELECT * FROM Product;"));
  }

  @Test
  @Disabled
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
  @Disabled
  public void distinctWithGroupNotInSelectTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := "
            + "  SELECT DISTINCT count(1) "
            + "  FROM _ JOIN Product p "
            + "  GROUP BY p.category;"));
  }

  @Test
  @Disabled
  public void topNTest() {
    Env env1 = generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := "
            + "  SELECT * "
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
  @Disabled
  public void localAggregateExpressionTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SUM(Product.productid);"));
  }

  @Test
  @Disabled
  public void localAggregateRelativePathTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SUM(joinDeclaration.productid);"));
  }

  @Test
  @Disabled
  public void compoundAggregateExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := SUM(_.productid) + SUM(_.productid);"));
  }

  @Test
  public void queryAsExpressionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SELECT SUM(x.productid) - 1 FROM _ AS x;"));
  }

  @Test
  @Disabled
  public void localAggregateInQueryTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SELECT SUM(joinDeclaration.productid) AS totals FROM _;"));
  }

  @Test
  @Disabled
  public void localAggregateCountTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
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
  @Disabled
  public void localAggregateCountStarTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := COUNT(*);"));
  }

  @Test
  @Disabled
  public void localAggregateCountStarTest2() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := COUNT();"));
  }

  @Test
  @Disabled
  public void localAggregateInAggregateTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SUM(COUNT(joinDeclaration));"));
  }

  @Test
  @Disabled
  public void parameterizedLocalAggregateTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
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
  @Disabled
  public void invalidInlinePathMultiplicityTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.joinDeclaration := JOIN Product ON true;\n"
            + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
  }

  @Test
  @Disabled
  public void inlinePathMultiplicityTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON true LIMIT 1;\n"
        + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
  }

  @Test
  @Disabled
  public void distinctOnTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := DISTINCT Product ON productid;\n"));
  }

  @Test
  @Disabled
  public void distinctOnWithExpression2Test() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;\n"
            + "Product2 := DISTINCT Orders ON id ORDER BY time DESC;\n"));
  }

  @Test
  @Disabled
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
  @Disabled
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
  @Disabled
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
  @Disabled
  //TODO: disable paths in group
  public void invalidGroupTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.parent._uuid AS gp, min(e.unit_price) "
        + "     FROM Orders.entries AS e "
        + "     GROUP BY e.parent._uuid;"));
  }

  @Test
  @Disabled
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
  @Disabled
  public void queryAsExpressionSameNamedTest4() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT sum(x.productid) AS example FROM _ AS x HAVING example > 10;\n"));
  }

  @Test
  @Disabled
  public void queryTableFunctionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.search(text: Int) := "
            + "  SELECT name FROM _ WHERE name = :text;\n"));
  }

  @Test
  @Disabled
  public void joinTableFunctionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "search(text: Int) := "
            + "  JOIN Product ON name = :text;\n"));
  }

  @Test
  @Disabled
  public void expressionTableFunctionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example(text: Int) := COALESCE(name, CAST('false' AS BOOLEAN));\n"));
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
  @Disabled
  public void intervalJoinTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Orders;"
            + "IMPORT ecommerce-data.Product;"
        + "X := SELECT * FROM Product AS p "
            + " INTERVAL JOIN Orders AS o "
            + "  ON true;"
    ));
  }

  @Test
  @Disabled
  public void castTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT CAST(1 AS String) From Orders;"));
  }

  @Test
  @Disabled
  public void castExpression() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.x := CAST(1 AS String);"));
  }

  @Test
  @Disabled
  public void aggregateIsToOne() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(1) AS num, SUM(e.discount) AS total FROM _ JOIN _.entries e;\n"
        + "X := SELECT o.id, o.customerid, o.stats.num FROM Orders o;"
    ));
  }

  @Test
  @Disabled
  public void aggregateIsToOne2() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "Orders.stats := SELECT COUNT(e.unit_price) AS num, SUM(e.discount) AS total FROM _.entries e;\n"
        + "X := SELECT o.id, o.customerid, o.stats.num FROM Orders o;"));
  }

  @Test
  @Disabled
  public void testc360() {
    generate(parser.parse("IMPORT ecommerce-data.Customer;\n"
        + "IMPORT ecommerce-data.Product;\n"
        + "IMPORT ecommerce-data.Orders;\n"
        + "\n"
        + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
        + "\n"
        + "-- Compute useful statistics on orders\n"
        + "Orders.entries.discount := coalesce(discount, 0.0);\n"
        + "Orders.entries.total := quantity * unit_price - discount;\n"
        + "Orders._stats := SELECT sum(total) AS total, sum(discount) AS total_savings, "
        + "                 COUNT(1) AS total_entries "
        + "                 FROM _.entries;\n"
        + "Orders.total := _.stats.total;\n"
        + "Orders.total_savings := _._stats.total_savings;\n"
        + "Orders.total_entries := _._stats.total_entries;\n"
        + "\n"
        + "\n"
        + "-- Relate Customer to Orders and compute a customer's total order spent\n"
        + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
        + "Customer._stats := SELECT sum(orders.total) AS total_orders FROM _;\n"
        + "Customer.total_orders := _._stats.total_orders;\n"
        + "\n"
        + "-- Aggregate all products the customer has ordered for the 'order again' feature\n"
        + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
        + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
        + "\n"
        + "Customer.recent_products := SELECT productid, product.category AS category,\n"
        + "                                   sum(quantity) AS quantity, count(*) AS num_orders\n"
        + "                            FROM _.orders.entries\n"
        + "                            WHERE parent.time > now() - INTERVAL 2 YEAR\n"
        + "                            GROUP BY productid, category ORDER BY num_orders DESC, quantity DESC;\n"
        + "\n"
        + "Customer.recent_products_categories :=\n"
        + "                     SELECT category, count(*) AS num_products\n"
        + "                     FROM _.recent_products\n"
        + "                     GROUP BY category ORDER BY num_products;\n"
        + "\n"
        + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp.category=_.category;\n"
        + "\n"
        + "-- Aggregate customer spending by month and product category for the 'spending history' feature\n"
        + "Customer._spending_by_month_category :=\n"
        + "                     SELECT time.roundToMonth(parent.time) AS month,\n"
        + "                            product.category AS category,\n"
        + "                            sum(total) AS total,\n"
        + "                            sum(discount) AS savings\n"
        + "                     FROM _.orders.entries\n"
        + "                     GROUP BY month, category ORDER BY month DESC;\n"
        + "\n"
        + "Customer.spending_by_month :=\n"
        + "                    SELECT month, sum(total) AS total, sum(savings) AS savings\n"
        + "                    FROM _._spending_by_month_category\n"
        + "                    GROUP BY month ORDER BY month DESC;\n"
        + "Customer.spending_by_month.categories :=\n"
        + "    JOIN _.parent._spending_by_month_category c ON c.month=month;\n"
        + "\n"
        + "/* Compute w/w product sales volume increase average over a month\n"
        + "   These numbers are internal to determine trending products */\n"
        + "Product._sales_last_week := SELECT SUM(e.quantity) AS total\n"
        + "                          FROM _.order_entries e\n"
        + "                          WHERE e.parent.time > now() - INTERVAL 7 DAY;\n"
        + "\n"
        + "Product._sales_last_month := SELECT SUM(e.quantity) AS total\n"
        + "                          FROM _.order_entries e\n"
        + "                          WHERE e.parent.time > now() - INTERVAL 1 MONTH;\n"
        + "\n"
        + "Product._last_week_increase := _sales_last_week.total * 4 / _sales_last_month.total;\n"
        + "\n"
        + "-- Determine trending products for each category\n"
        + "Category := SELECT DISTINCT category AS name FROM Product;\n"
        + "Category.products := JOIN Product ON _.name = Product.category;\n"
        + "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase > 0\n"
        + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n"
        + "\n"
        + "/* Determine customers favorite categories by total spent\n"
        + "   In combination with trending products this is used for the product recommendation feature */\n"
        + "Customer.favorite_categories := SELECT s.category as category_name,\n"
        + "                                        sum(s.total) AS total\n"
        + "                                FROM _._spending_by_month_category s\n"
        + "                                WHERE s.month >= now() - INTERVAL 1 YEAR\n"
        + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n"
        + "\n"
        + "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n"
        + "\n"));
  }

  @Test
  @Ignore
  public void invalidDistinctTest1() {
    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Customer;\n");
//    node = gen("Customer.nested := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;
//    \n");
//    System.out.println(node);
  }
//
//  @Test
//  public void invalidDistinctTest2() {
//    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Orders;\n");
//    node = gen("Customer := DISTINCT Orders.entries ON _idx;\n");
//    System.out.println(node);
//  }
//  @Test
//  public void invalidDistinctTest3() {
//    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Orders;\n");
//    node = gen("Customer := DISTINCT Orders o ON _uuid ORDER BY o.entries;\n");
//    System.out.println(node);
//  }
//  @Test
//  public void distinctWithAlias() {
//    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Customer;\n");
//    node = gen("Customer := DISTINCT Customer c ON c.customerid;\n");
//    System.out.println(node);
//  }
//  @Test
//  public void invalidNonAggregatingExpression() {
//    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Customer;\n");
//    node = gen("Customer.test := SELECT customerid+1 FROM _;");
//    System.out.println(node);
//  }

//  @Test
//  public void warnCrossJoinTest() {
//    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Orders;\n");
//    node = gen("Orders.warn := SELECT * FROM _ CROSS JOIN _.entries;");
//    System.out.println(node);
//  }
}
