package ai.datasqrl.plan.local.analyze;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.ParsingException;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.data.C360;
import java.io.IOException;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class AnalyzerTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;

  ErrorCollector error;

  private Session session;
  private Resolve resolve;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    Assertions.assertTrue(
        importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));
    Planner planner = new PlannerFactory(
        CalciteSchema.createRootSchema(false, false).plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve();
  }

  private void generate(ScriptNode node) {
    resolve.planDag(session, node);
  }

  private void generateInvalid(ScriptNode node) {
    assertThrows(Exception.class,
        ()->resolve.planDag(session, node),
        "Statement should throw exception"
        );
  }

  @Test
  public void pathTest() {
    generate(parser.parse("IMPORT ecommerce-data.Orders;"
        + "X := SELECT e.* FROM Orders.entries AS e JOIN e.parent p;"));
  }

  @Test
  public void importTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;"));
  }

  @Test
  public void duplicateImportTest() {
    generate(
        parser.parse("IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Product;\n"));
  }

  @Test
  @Disabled
  public void importAllTest() {
    generate(parser.parse("IMPORT ecommerce-data.*;"));
  }

  @Test
  @Disabled
  public void importAllWithAliasTest() {
    generate(parser.parse("IMPORT ecommerce-data.* AS ecommerce;"));
  }

  @Test
  @Disabled
  public void importWithTimestamp() {
    generate(parser.parse("IMPORT ecommerce-data.Product TIMESTAMP uuid;"));
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
            + "Product.productCopy := SELECT * FROM Product;"));
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
  public void fullyQualifiedQueryName() {
    generate(parser.parse(
        "IMPORT ecommerce-data;\n"
            + "Product2 := SELECT * FROM ecommerce-data.Product;"));
  }

  @Test
  @Disabled
  public void invalidShadowRelationshipTest() {
    generateInvalid(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.productCopy := SELECT * FROM Product;"
            + "Product.productCopy := SELECT * FROM Product;"));
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
        + "Product.joinDeclaration.column := SELECT * FROM Product;"));
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
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "NewProduct := SELECT joinDeclaration.productid FROM Product;"));
  }

  @Test
  @Disabled
  public void parentTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "NewProduct := SELECT parent.productid FROM Product.joinDeclaration;"));
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
  public void crossJoinTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product2 := SELECT * FROM Product, Product.joinDeclaration;"));
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
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT _ingest_time + INTERVAL 2 YEAR AS x FROM Product;"));
  }

  @Test
  @Disabled
  public void distinctStarTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product2 := SELECT DISTINCT * FROM Product;"));
  }

  @Test
  public void distinctSingleColumnTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product2 := SELECT DISTINCT productid FROM Product;"));
  }

  @Test
  @Disabled
  public void localAggregateExpressionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SUM(Product.productid);"));
  }

  @Test
  public void localAggregateRelativePathTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
        + "Product.total := SUM(joinDeclaration.productid);"));
  }

  @Test
  public void compoundAggregateExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.total := SUM(_.productid) + SUM(_.productid);"));
  }

  @Test
  public void queryAsExpressionTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.total := SELECT SUM(productid) - 1 FROM _;"));
  }

  @Test
  @Disabled
  public void localAggregateInQueryTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product"
        + ".productid;\n"
        + "Product.total := SELECT SUM(joinDeclaration.productid) FROM _;"));
  }

  @Test
  public void localAggregateCountTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product"
        + ".productid;\n"
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
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product"
        + ".productid;\n"
        + "Product.total := COUNT(*);"));
  }

  @Test
  public void localAggregateInAggregateTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = Product"
        + ".productid;\n"
        + "Product.total := SUM(COUNT(joinDeclaration));"));
  }

  @Test
  public void parameterizedLocalAggregateTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = "
        + "Product.productid LIMIT 1;\n"
        + "Product.total := COALESCE(joinDeclaration.productid, 1000);\n"));
  }

  @Test
  public void invalidParameterizedLocalAggregateTest() {
    generateInvalid(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.joinDeclaration := JOIN Product ON _.productid = "
        + "Product.productid;\n"
        + "Product.total := MIN(joinDeclaration.productid, "
        + "joinDeclaration.parent.productid);\n"));
  }

  @Test
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
        + "Product2 := SELECT joinDeclaration.productid, productid FROM"
        + " Product;\n"));
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
  public void nestedLocalDistinctTest() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.nested := SELECT productid FROM Product;"
            + "Product.nested := DISTINCT _ ON productid;"));
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
        + "Product2 := SELECT * FROM Product ORDER BY productid / 10;"
        + "\n"));
  }

  @Test
  public void queryNotAsExpressionTest() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.example := SELECT productid FROM Product;\n"));
  }

  @Test
  public void queryAsExpressionTest2() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT productid FROM _;\n"));
  }

  @Test
  public void queryAsExpressionUnnamedTest3() {
    generate(parser.parse("IMPORT ecommerce-data.Product;\n"
        + "Product.example := SELECT _.productid + 1 FROM _ INNER "
        + "JOIN Product ON true;\n"));
  }

  @Test
  @Disabled
  public void queryAsExpressionSameNamedTest4() {
    generate(parser.parse(
        "IMPORT ecommerce-data.Product;\n"
            + "Product.example := SELECT sum(productid) AS example "
            + "FROM _ HAVING example > 10;\n"));
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
  @Ignore
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
    String query = "SELECT t.* FROM Product j JOIN Product h ON "
        + "true;";
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
