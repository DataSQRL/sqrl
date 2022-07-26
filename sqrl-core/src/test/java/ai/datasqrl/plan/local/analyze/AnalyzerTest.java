//package ai.datasqrl.plan.local.analyze;
//
//import ai.datasqrl.AbstractSQRLIT;
//import ai.datasqrl.IntegrationTestSettings;
//import ai.datasqrl.config.error.ErrorCollector;
//import ai.datasqrl.config.scripts.ScriptBundle;
//import ai.datasqrl.environment.ImportManager;
//import ai.datasqrl.parse.ConfiguredSqrlParser;
//import ai.datasqrl.parse.tree.name.Name;
//import ai.datasqrl.schema.Column;
//import ai.datasqrl.schema.Field;
//import ai.datasqrl.schema.Relationship;
//import ai.datasqrl.schema.Relationship.Multiplicity;
//import ai.datasqrl.schema.VarTable;
//import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
//import ai.datasqrl.util.data.C360;
//import org.apache.calcite.sql.SqlNode;
//import org.junit.Ignore;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.util.Optional;
//
//@Disabled
//class AnalyzerTest extends AbstractSQRLIT {
//
//  ConfiguredSqrlParser parser;
//
//  ErrorCollector errorCollector;
//  ImportManager importManager;
//  Analyzer analyzer;
//
//  @BeforeEach
//  public void setup() throws IOException {
//    errorCollector = ErrorCollector.root();
//    initialize(IntegrationTestSettings.getInMemory(false));
//    C360 example = C360.INSTANCE;
//
//    example.registerSource(env);
//
//    importManager = sqrlSettings.getImportManagerProvider()
//        .createImportManager(env.getDatasetRegistry());
//    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
//    Assertions.assertTrue(importManager.registerUserSchema(bundle.getMainScript().getSchema(),
//        ErrorCollector.root()));
//    parser = ConfiguredSqrlParser.newParser(errorCollector);
//    analyzer = new Analyzer(importManager, SchemaAdjustmentSettings.DEFAULT,
//        errorCollector);
//  }
//
//  @Test
//  public void importTest() {
//    Analysis analysis = analyzer.analyze(parser.parse("IMPORT ecommerce-data.Product;"));
//
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Product")).isPresent());
//  }
//
//  @Test
//  public void duplicateImportTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//        "IMPORT ecommerce-data.Product;\n"
//    ));
//
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Product")).isPresent());
//  }
//
//  @Test
//  public void importAllTest() {
//    Analysis analysis = analyzer.analyze(parser.parse("IMPORT ecommerce-data.*;"));
//
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Product")).isPresent());
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Customer")).isPresent());
//  }
//
//  @Test
//  public void importAllWithAliasTest() {
//    Analysis analysis = analyzer.analyze(parser.parse("IMPORT ecommerce-data.* AS ecommerce;"));
//
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Product")).isPresent());
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Customer")).isPresent());
//  }
//
//  @Test
//  public void importWithTimestamp() {
//    Analysis analysis = analyzer.analyze(parser.parse("IMPORT ecommerce-data.Product TIMESTAMP uuid;"));
//
//    Assertions.assertTrue(analysis.getSchema().getTable(Name.system("Product")).isPresent());
//  }
//
//  @Test
//  public void invalidImportDuplicateAliasTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//        "IMPORT ecommerce-data.Customer AS Product;\n"
//    ));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void expressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//        "Product.descriptionLength := CHAR_LENGTH(description);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Assertions.assertTrue(productTable.get().getField(Name.system("descriptionLength")).isPresent());
//  }
//  @Test
//  public void shadowExpressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//        "Product.descriptionLength := CHAR_LENGTH(description);" +
//        "Product.descriptionLength := CHAR_LENGTH(description);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Assertions.assertTrue(productTable.get().getField(Name.system("descriptionLength")).isPresent());
//    Assertions.assertEquals(productTable.get().getFields().numFields(), 8);
//  }
//
//  @Test
//  public void selectStarQueryTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//        "ProductCopy := SELECT * FROM Product;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("ProductCopy"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Assertions.assertTrue(productTable.get().getField(Name.system("productid")).isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("description")).isPresent());
//  }
//
//  @Test
//  public void nestedCrossJoinQueryTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.productCopy := SELECT * FROM Product;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Optional<Field> productCopyRelationship = productTable.get().getField(Name.system("productCopy"));
//    Assertions.assertTrue(productCopyRelationship.isPresent());
//    Assertions.assertTrue(productCopyRelationship.get() instanceof Relationship);
//  }
//
//
//  @Test
//  public void nestedSelfJoinQueryTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.productSelf := SELECT * FROM _;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Optional<Field> selfRel = productTable.get().getField(Name.system("productSelf"));
//    Assertions.assertTrue(selfRel.isPresent());
//    Assertions.assertTrue(selfRel.get() instanceof Relationship);
//
//    Relationship rel = (Relationship) selfRel.get();
//
//    Assertions.assertEquals(rel.getMultiplicity(), Multiplicity.ONE);
//  }
//
//  @Test
//  public void invalidRootExpressionTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "ProductCount := count(Product);"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void fullyQualifiedQueryName() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product2 := SELECT * FROM ecommerce-data.Product;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void invalidShadowRelationshipTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.productCopy := SELECT * FROM Product;" +
//            "Product.productCopy := SELECT * FROM Product;"
//    ));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void testJoinDeclaration() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product := DISTINCT Product ON productid;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Optional<Field> selfRel = productTable.get().getField(Name.system("joinDeclaration"));
//    Assertions.assertTrue(selfRel.isPresent());
//    Assertions.assertTrue(selfRel.get() instanceof Relationship);
//
//    Relationship rel = (Relationship) selfRel.get();
//
//    Assertions.assertEquals(rel.getMultiplicity(), Multiplicity.ONE);
//  }
//
//  @Test
//  public void testOrderedJoinDeclaration() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product := DISTINCT Product ON productid;\n" +
//            "Product.joinDeclaration := JOIN Product ON true ORDER BY Product.productid;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//
//    Optional<Field> selfRel = productTable.get().getField(Name.system("joinDeclaration"));
//    Assertions.assertTrue(selfRel.isPresent());
//    Assertions.assertTrue(selfRel.get() instanceof Relationship);
//
//    Relationship rel = (Relationship) selfRel.get();
//
////    Assertions.assertTrue(rel.getOrders().isPresent());
//  }
//
//  @Test
//  public void testJoinDeclarationOnRoot() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product2 := JOIN Product ON _.productid = Product.productid;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void invalidExpressionAssignmentOnRelationshipTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.joinDeclaration.column := 1;"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void invalidQueryAssignmentOnRelationshipTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.joinDeclaration.column := SELECT * FROM Product;"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void invalidShadowJoinDeclarationTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void tablePathTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "NewProduct := SELECT * FROM Product.joinDeclaration;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("NewProduct"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void inlinePathTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "NewProduct := SELECT joinDeclaration.productid FROM Product;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("NewProduct"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void parentTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "NewProduct := SELECT parent.productid FROM Product.joinDeclaration;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("NewProduct"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
////
////  @Test
////  public void joinDeclarationShadowTest() {
////    Analysis analysis = analyzer.analyze(parser.parse(
////        "IMPORT ecommerce-data.Product;\n" +
////            "PointsToProduct := SELECT * FROM Product ON _.productid = Product.productid;\n" +
////            "Product := SELECT 1 AS x, 2 AS y FROM Product;\n" +
////            "OldProduct := SELECT * FROM PointsToProduct;"));
////
////    Optional<Table> pointsToProduct = analysis.getSchema().getTable(Name.system("PointsToProduct"));
////    Assertions.assertTrue(pointsToProduct.isPresent());
////    Assertions.assertEquals(pointsToProduct.get().getFields().size(), 4);
////
////    Optional<Table> product = analysis.getSchema().getTable(Name.system("Product"));
////    Assertions.assertTrue(product.isPresent());
////    Assertions.assertEquals(pointsToProduct.get().getFields().size(), 2);
////
////    Optional<Table> oldProduct = analysis.getSchema().getTable(Name.system("OldProduct"));
////    Assertions.assertTrue(oldProduct.isPresent());
////    Assertions.assertEquals(pointsToProduct.get().getFields().size(), 4);
////  }
//
//  @Test
//  public void invalidRelationshipInColumnTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT joinDeclaration FROM Product;"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void subQueryExpressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT * FROM Product WHERE productid IN (SELECT productid FROM Product);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//
//  @Test
//  public void crossJoinTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT * FROM Product, Product.joinDeclaration;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void subQueryTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM Product) f;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void invalidSelfInSubqueryTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT * FROM Product, (SELECT MIN(productid) FROM _);"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void unionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT * FROM Product UNION SELECT * FROM Product.joinDeclaration;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void unionAllTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product2 := SELECT * FROM Product UNION ALL SELECT * FROM Product.joinDeclaration;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void distinctStarTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product2 := SELECT DISTINCT * FROM Product;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void distinctSingleColumnTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product2 := SELECT DISTINCT productid FROM Product;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void localAggregateExpressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.total := SUM(Product.productid);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void localAggregateRelativePathTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := SUM(joinDeclaration.productid);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void compoundAggregateExpressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.total := SUM(Product.productid) + SUM(Product.productid);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void queryAsExpressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.total := SELECT SUM(productid) - 1 FROM _;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void localAggregateInQueryTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := SELECT SUM(joinDeclaration.productid) FROM _;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void localAggregateCountTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := COUNT(joinDeclaration);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void compoundJoinDeclarations() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.joinDeclaration2 := JOIN _.joinDeclaration j ON _.productid = j.productid;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void localAggregateCountStarTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := COUNT(*);"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void localAggregateInAggregateTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := SUM(COUNT(joinDeclaration));"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void parameterizedLocalAggregateTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := MIN(joinDeclaration.productid, 1000);\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("total")).isPresent());
//  }
//
//  @Test
//  public void invalidParameterizedLocalAggregateTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON _.productid = Product.productid;\n" +
//            "Product.total := MIN(joinDeclaration.productid, joinDeclaration.parent.productid);\n"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void invalidInlinePathMultiplicityTest() {
//    analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON true;\n" +
//            "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
//
//    Assertions.assertTrue(errorCollector.hasErrors());
//  }
//
//  @Test
//  public void inlinePathMultiplicityTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.joinDeclaration := JOIN Product ON true LIMIT 1;\n" +
//            "Product2 := SELECT joinDeclaration.productid, productid FROM Product;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void distinctOnTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product2 := DISTINCT Product ON productid;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void nestedLocalDistinctTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.nested := SELECT productid FROM Product;" +
//            "Product.nested := DISTINCT _ ON productid;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  @Ignore
//  public void nestedDistinctTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.nested := SELECT productid FROM Product;" +
//            "Product2 := DISTINCT Product.nested ON productid;"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void uniqueOrderByTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product2 := SELECT * FROM Product ORDER BY productid / 10;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product2"));
//    Assertions.assertTrue(productTable.isPresent());
//  }
//
//  @Test
//  public void queryNotAsExpressionTest() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.example := SELECT productid FROM Product;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("example")).get() instanceof Relationship);
//  }
//
//  @Test
//  public void queryAsExpressionTest2() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.example := SELECT productid FROM _;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("example")).get() instanceof Column);
//  }
//
//  @Test
//  public void queryAsExpressionUnnamedTest3() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.example := SELECT _.productid + 1 FROM _ INNER JOIN Product ON true;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("example")).get() instanceof Relationship);
//  }
//
//  @Test
//  public void queryAsExpressionSameNamedTest4() {
//    Analysis analysis = analyzer.analyze(parser.parse(
//        "IMPORT ecommerce-data.Product;\n" +
//            "Product.example := SELECT sum(productid) AS example FROM _ HAVING example > 10;\n"));
//
//    Optional<VarTable> productTable = analysis.getSchema().getTable(Name.system("Product"));
//    Assertions.assertTrue(productTable.isPresent());
//    Assertions.assertTrue(productTable.get().getField(Name.system("example")).get() instanceof Relationship);
//  }
//
//  @Test
//  public void starAliasPrefixTest() {
//    String query = "SELECT t.* FROM Product j JOIN Product h ON true;";
//  }
//  @Test
//  public void fullTest() {
//
//    String query = "IMPORT ecommerce-data.Customer;\n"
//        + "IMPORT ecommerce-data.Product;\n"
//        + "IMPORT ecommerce-data.Orders;\n"
//        + "\n"
//        + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
//        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
//        + "\n"
//        + "-- Compute useful statistics on orders\n"
//        + "Orders.entries.discount := coalesce(discount, 0.0);\n"
//        + "Orders.entries.total := quantity * unit_price - discount;\n"
//        + "Orders.total := sum(entries.total);\n"
//        + "Orders.total_savings := sum(entries.discount);\n"
//        + "Orders.total_entries := count(entries);\n"
//        + "\n"
//        + "\n"
//        + "-- Relate Customer to Orders and compute a customer's total order spent\n"
//        + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
//        + "Customer.total_orders := sum(_.orders.total);\n"
//        + "\n"
//        + "-- Aggregate all products the customer has ordered for the 'order again' feature\n"
//        + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
//        + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
//        + "\n"
//        + "Customer.recent_products := SELECT productid, e.product.category AS category,\n"
//        + "                                   sum(quantity) AS quantity, count(*) AS num_orders\n"
//        + "                            FROM _.orders.entries e\n"
//        + "                            WHERE parent.time > now() - INTERVAL 2 YEAR\n"
//        + "                            GROUP BY productid, category ORDER BY num_orders DESC, "
//        + "quantity DESC;\n"
//        + "\n"
//        + "Customer.recent_products_categories :=\n"
//        + "                     SELECT category, count(*) AS num_products\n"
//        + "                     FROM _.recent_products\n"
//        + "                     GROUP BY category ORDER BY num_products;\n"
//        + "\n"
//        + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp"
//        + ".category=_.category;\n"
//        + "\n"
//        + "-- Aggregate customer spending by month and product category for the 'spending "
//        + "history' feature\n"
//        + "Customer._spending_by_month_category :=\n"
//        + "                     SELECT time.roundToMonth(parent.time) AS month,\n"
//        + "                            e.product.category AS category,\n"
//        + "                            sum(total) AS total,\n"
//        + "                            sum(discount) AS savings\n"
//        + "                     FROM _.orders.entries e\n"
//        + "                     GROUP BY month, category ORDER BY month DESC;\n"
//        + "\n"
//        + "Customer.spending_by_month :=\n"
//        + "                    SELECT month, sum(total) AS total, sum(savings) AS savings\n"
//        + "                    FROM _._spending_by_month_category\n"
//        + "                    GROUP BY month ORDER BY month DESC;\n"
//        + "Customer.spending_by_month.categories :=\n"
//        + "    JOIN _.parent._spending_by_month_category c ON c.month=month;\n"
//        + "\n"
//        + "/* Compute w/w product sales volume increase average over a month\n"
//        + "   These numbers are internal to determine trending products */\n"
//        + "Product._sales_last_week := SELECT SUM(e.quantity)\n"
//        + "                          FROM _.order_entries e\n"
//        + "                          --WHERE e.parent.time > now() - INTERVAL 1 WEEK;\n"
//        + "                          WHERE e.parent.time > now() - INTERVAL 7 DAY;\n"
//        + "\n"
//        + "Product._sales_last_month := SELECT SUM(e.quantity)\n"
//        + "                          FROM _.order_entries e\n"
//        + "                          --WHERE e.parent.time > now() - INTERVAL 4 WEEK;\n"
//        + "                          WHERE e.parent.time > now() - INTERVAL 1 MONTH;\n"
//        + "\n"
//        + "Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n"
//        + "\n"
//        + "-- Determine trending products for each category\n"
//        + "Category := SELECT DISTINCT category AS name FROM Product;\n"
//        + "Category.products := JOIN Product ON _.name = Product.category;\n"
//        + "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase >"
//        + " 0\n"
//        + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n"
//        + "\n"
//        + "/* Determine customers favorite categories by total spent\n"
//        + "   In combination with trending products this is used for the product recommendation "
//        + "feature */\n"
//        + "Customer.favorite_categories := SELECT s.category as category_name,\n"
//        + "                                        sum(s.total) AS total\n"
//        + "                                FROM _._spending_by_month_category s\n"
//        + "                                WHERE s.month >= now() - INTERVAL 1 YEAR\n"
//        + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n"
//        + "\n"
//        + "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category"
//        + ".name;\n"
//        + "\n"
//        + "-- Create subscription for customer spending more than $100 so we can send them a "
//        + "coupon --\n"
//        + "\n"
//        + "CREATE SUBSCRIPTION NewCustomerPromotion ON ADD AS\n"
//        + "SELECT customerid, email, name, total_orders FROM Customer WHERE total_orders >= 100;\n";
//
//    Analysis analysis = analyzer.analyze(parser.parse(query));
//
//  }
//
//  @Test
//  public void invalidDistinctOrder() {
//
//  }
//  @Test
//  public void invalidAliasJoinOrder() {
//    String query = "SELECT * From Orders o JOIN o;";
//  }
//
//
//  @Test
//  public void invalidDistinctTest1() {
//    SqlNode node;
////    node = gen("IMPORT ecommerce-data.Customer;\n");
////    node = gen("Customer.nested := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n");
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
//}
  //
//  @Test
//  public void warnCrossJoinTest() {
//    SqlNode node;
//    node = gen("IMPORT ecommerce-data.Orders;\n");
//    node = gen("Orders.warn := SELECT * FROM _ CROSS JOIN _.entries;");
//    System.out.println(node);
//  }
  //
//}