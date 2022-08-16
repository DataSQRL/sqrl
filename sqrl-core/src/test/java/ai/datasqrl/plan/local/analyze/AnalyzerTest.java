//package ai.datasqrl.plan.local.analyze;
//
//import ai.datasqrl.AbstractSQRLIT;
//import ai.datasqrl.IntegrationTestSettings;
//import ai.datasqrl.config.error.ErrorCollector;
//import ai.datasqrl.config.scripts.ScriptBundle;
//import ai.datasqrl.environment.ImportManager;
//import ai.datasqrl.parse.ConfiguredSqrlParser;
//import ai.datasqrl.plan.local.generate.Generator;
//import ai.datasqrl.plan.local.generate.GeneratorBuilder;
//import ai.datasqrl.util.data.C360;
//import java.io.IOException;
//import org.apache.calcite.sql.SqlNode;
//import org.junit.Ignore;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//
//@Disabled
//class AnalyzerTest extends AbstractSQRLIT {
//
//  ConfiguredSqrlParser parser;
//
//  ErrorCollector error;
//
//  Generator generator;
//
//  @BeforeEach
//  public void setup() throws IOException {
//    error = ErrorCollector.root();
//    initialize(IntegrationTestSettings.getInMemory(false));
//    C360 example = C360.INSTANCE;
//    example.registerSource(env);
//
//    ImportManager importManager = sqrlSettings.getImportManagerProvider()
//        .createImportManager(env.getDatasetRegistry());
//    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
//    importManager.registerUserSchema(bundle.getMainScript().getSchema(),
//        error);
//
//    this.generator = GeneratorBuilder.build(importManager, error);
//    this.parser = new ConfiguredSqrlParser(error);
//  }
//
//  @Test
//  public void importTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;"));
//
//  }
//
//  @Test
//  public void duplicateImportTest() {
//    generator.generate(
//        parser.parse("IMPORT ecommerce-data.Product;\n" + "IMPORT ecommerce-data.Product;\n"));
//
//  }
//
//  @Test
//  public void importAllTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.*;"));
//
//  }
//
//  @Test
//  public void importAllWithAliasTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.* AS ecommerce;"));
//
//  }
//
//  @Test
//  public void importWithTimestamp() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product TIMESTAMP uuid;"));
//
//  }
//
//  @Test
//  public void invalidImportDuplicateAliasTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "IMPORT ecommerce-data.Customer AS Product;\n"));
//
//  }
//
//  @Test
//  public void expressionTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.descriptionLength := CHAR_LENGTH(description);"));
//
//
//  }
//
//  @Test
//  public void shadowExpressionTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.descriptionLength := CHAR_LENGTH(description);"
//        + "Product.descriptionLength := CHAR_LENGTH(description);"));
//
//
//  }
//
//  @Test
//  public void selectStarQueryTest() {
//    generator.generate(
//        parser.parse("IMPORT ecommerce-data.Product;\n" + "ProductCopy := SELECT * FROM Product;"));
//
//
//  }
//
//  @Test
//  public void nestedCrossJoinQueryTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.productCopy := SELECT * FROM Product;"));
//  }
//
//  @Test
//  public void nestedSelfJoinQueryTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.productSelf := SELECT * FROM _;"));
//  }
//
//  @Test
//  public void invalidRootExpressionTest() {
//    generator.generate(
//        parser.parse("IMPORT ecommerce-data.Product;\n" + "ProductCount := count(Product);"));
//
//  }
//
//  @Test
//  public void fullyQualifiedQueryName() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product2 := SELECT * FROM ecommerce-data.Product;"));
//
//
//  }
//
//  @Test
//  public void invalidShadowRelationshipTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.productCopy := SELECT * FROM Product;"
//            + "Product.productCopy := SELECT * FROM Product;"));
//
//  }
//
//  @Test
//  public void testJoinDeclaration() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product := DISTINCT Product ON productid;\n"
//            + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;"));
//  }
//
//  @Test
//  public void testOrderedJoinDeclaration() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product := DISTINCT Product ON productid;\n"
//            + "Product.joinDeclaration := JOIN Product ON true ORDER BY Product.productid;"));
//  }
//
//  @Test
//  public void testJoinDeclarationOnRoot() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product2 := JOIN Product ON _.productid = Product.productid;"));
//
//
//  }
//
//  @Test
//  public void invalidExpressionAssignmentOnRelationshipTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product.joinDeclaration.column := 1;"));
//
//  }
//
//  @Test
//  public void invalidQueryAssignmentOnRelationshipTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product.joinDeclaration.column := SELECT * FROM Product;"));
//
//  }
//
//  @Test
//  public void invalidShadowJoinDeclarationTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;"));
//
//  }
//
//  @Test
//  public void tablePathTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "NewProduct := SELECT * FROM Product.joinDeclaration;"));
//
//
//  }
//
//  @Test
//  public void inlinePathTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "NewProduct := SELECT joinDeclaration.productid FROM Product;"));
//
//
//  }
//
//  @Test
//  public void parentTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "NewProduct := SELECT parent.productid FROM Product.joinDeclaration;"));
//
//
//  }
////
////  @Test
////  public void joinDeclarationShadowTest() {
////    generator.generate(parser.parse(
////        "IMPORT ecommerce-data.Product;\n" +
////            "PointsToProduct := SELECT * FROM Product ON _.productid = Product.productid;\n" +
////            "Product := SELECT 1 AS x, 2 AS y FROM Product;\n" +
////            "OldProduct := SELECT * FROM PointsToProduct;"));
////
////    Optional<Table> pointsToProduct = generator.getSqrlSchema().getTable("PointsToProduct",
////    false)
////    //    Assertions.assertEquals(pointsToProduct.getFields().size(), 4);
////
////    Optional<Table> product = generator.getSqrlSchema().getTable("Product", false)
////    //    Assertions.assertEquals(pointsToProduct.getFields().size(), 2);
////
////    Optional<Table> oldProduct = generator.getSqrlSchema().getTable("OldProduct", false)
////    //    Assertions.assertEquals(pointsToProduct.getFields().size(), 4);
////  }
//
//  @Test
//  public void invalidRelationshipInColumnTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT joinDeclaration FROM Product;"));
//
//  }
//
//  @Test
//  public void subQueryExpressionTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT * FROM Product WHERE productid IN (SELECT productid FROM "
//        + "Product);"));
//
//
//  }
//
//  @Test
//  public void crossJoinTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT * FROM Product, Product.joinDeclaration;"));
//
//
//  }
//
//  @Test
//  public void subQueryTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM Product) f;"));
//
//
//  }
//
//  @Test
//  public void invalidSelfInSubqueryTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM _);"));
//
//  }
//
//  @Test
//  public void unionTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT * FROM Product UNION SELECT * FROM Product.joinDeclaration;"));
//
//
//  }
//
//  @Test
//  public void unionAllTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product2 := SELECT * FROM Product UNION ALL SELECT * FROM Product"
//        + ".joinDeclaration;"));
//
//
//  }
//
//  @Test
//  public void distinctStarTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product2 := SELECT DISTINCT * FROM Product;"));
//
//
//  }
//
//  @Test
//  public void distinctSingleColumnTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product2 := SELECT DISTINCT productid FROM Product;"));
//
//
//  }
//
//  @Test
//  public void localAggregateExpressionTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.total := SUM(Product.productid);"));
//
//
//  }
//
//  @Test
//  public void localAggregateRelativePathTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product.total := SUM(joinDeclaration.productid);"));
//
//
//  }
//
//  @Test
//  public void compoundAggregateExpressionTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.total := SUM(Product.productid) + SUM(Product.productid);"));
//
//
//  }
//
//  @Test
//  public void queryAsExpressionTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.total := SELECT SUM(productid) - 1 FROM _;"));
//
//
//  }
//
//  @Test
//  public void localAggregateInQueryTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product" + ".productid;\n"
//        + "Product.total := SELECT SUM(joinDeclaration.productid) FROM _;"));
//
//
//  }
//
//  @Test
//  public void localAggregateCountTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product" + ".productid;\n"
//        + "Product.total := COUNT(joinDeclaration);"));
//
//
//  }
//
//  @Test
//  public void compoundJoinDeclarations() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n"
//        + "Product.joinDeclaration2 := JOIN _.joinDeclaration j ON _"
//        + ".productid = j.productid;\n"));
//
//
//  }
//
//  @Test
//  public void localAggregateCountStarTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product" + ".productid;\n"
//        + "Product.total := COUNT(*);"));
//
//
//  }
//
//  @Test
//  public void localAggregateInAggregateTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = Product" + ".productid;\n"
//        + "Product.total := SUM(COUNT(joinDeclaration));"));
//
//
//  }
//
//  @Test
//  public void parameterizedLocalAggregateTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = " + "Product.productid LIMIT 1;\n"
//        + "Product.total := MIN(joinDeclaration.productid, 1000);\n"));
//
//
//  }
//
//  @Test
//  public void invalidParameterizedLocalAggregateTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON _.productid = " + "Product.productid;\n"
//        + "Product.total := MIN(joinDeclaration.productid, "
//        + "joinDeclaration.parent.productid);\n"));
//
//  }
//
//  @Test
//  public void invalidInlinePathMultiplicityTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.joinDeclaration := JOIN Product ON true;\n"
//            + "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
//
//  }
//
//  @Test
//  public void inlinePathMultiplicityTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.joinDeclaration := JOIN Product ON true LIMIT 1;\n"
//        + "Product2 := SELECT joinDeclaration.productid, productid FROM" + " Product;\n"));
//
//
//  }
//
//  @Test
//  public void distinctOnTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product2 := DISTINCT Product ON productid;\n"));
//
//
//  }
//
//  @Test
//  public void nestedLocalDistinctTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.nested := SELECT productid FROM Product;"
//            + "Product.nested := DISTINCT _ ON productid;"));
//
//
//  }
//
//  @Test
//  @Ignore
//  public void nestedDistinctTest() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.nested := SELECT productid FROM Product;"
//            + "Product2 := DISTINCT Product.nested ON productid;"));
//
//
//  }
//
//  @Test
//  public void uniqueOrderByTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product2 := SELECT * FROM Product ORDER BY productid / 10;" + "\n"));
//
//
//  }
//
//  @Test
//  public void queryNotAsExpressionTest() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.example := SELECT productid FROM Product;\n"));
//
//
//  }
//
//  @Test
//  public void queryAsExpressionTest2() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.example := SELECT productid FROM _;\n"));
//
//
//  }
//
//  @Test
//  public void queryAsExpressionUnnamedTest3() {
//    generator.generate(parser.parse("IMPORT ecommerce-data.Product;\n"
//        + "Product.example := SELECT _.productid + 1 FROM _ INNER " + "JOIN Product ON true;\n"));
//
//
//  }
//
//  @Test
//  public void queryAsExpressionSameNamedTest4() {
//    generator.generate(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" + "Product.example := SELECT sum(productid) AS example "
//            + "FROM _ HAVING example > 10;\n"));
//  }
//
//  @Test
//  public void starAliasPrefixTest() {
//    String query = "SELECT t.* FROM Product j JOIN Product h ON " + "true;";
//  }
//
//  @Test
//  public void invalidDistinctOrder() {
//
//  }
//
//  @Test
//  public void invalidAliasJoinOrder() {
//    String query = "SELECT * From Orders o JOIN o;";
//  }
//
//  @Test
//  public void invalidDistinctTest1() {
//    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Customer;\n");
////    node = gen("Customer.nested := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;
////    \n");
////    System.out.println(node);
//  }
////
////  @Test
////  public void invalidDistinctTest2() {
////    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Orders;\n");
////    node = gen("Customer := DISTINCT Orders.entries ON _idx;\n");
////    System.out.println(node);
////  }
////  @Test
////  public void invalidDistinctTest3() {
////    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Orders;\n");
////    node = gen("Customer := DISTINCT Orders o ON _uuid ORDER BY o.entries;\n");
////    System.out.println(node);
////  }
////  @Test
////  public void distinctWithAlias() {
////    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Customer;\n");
////    node = gen("Customer := DISTINCT Customer c ON c.customerid;\n");
////    System.out.println(node);
////  }
////  @Test
////  public void invalidNonAggregatingExpression() {
////    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Customer;\n");
////    node = gen("Customer.test := SELECT customerid+1 FROM _;");
////    System.out.println(node);
////  }
//
////  @Test
////  public void warnCrossJoinTest() {
////    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Orders;\n");
////    node = gen("Orders.warn := SELECT * FROM _ CROSS JOIN _.entries;");
////    System.out.println(node);
////  }
//
//}
