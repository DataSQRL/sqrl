//package ai.datasqrl.sqrl2sql;
//
//import ai.datasqrl.C360Example;
//import ai.datasqrl.Environment;
//import ai.datasqrl.api.ConfigurationTest;
//import ai.datasqrl.config.SqrlSettings;
//import ai.datasqrl.config.error.ErrorCollector;
//import ai.datasqrl.config.scripts.ScriptBundle;
//import ai.datasqrl.config.scripts.SqrlScript;
//import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
//import ai.datasqrl.parse.SqrlParser;
//import ai.datasqrl.parse.tree.Node;
//import ai.datasqrl.parse.tree.ScriptNode;
//import ai.datasqrl.plan.local.operations.SchemaBuilder;
//import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
//import ai.datasqrl.server.ImportManager;
//import ai.datasqrl.plan.local.SchemaUpdatePlanner;
//import ai.datasqrl.plan.local.transpiler.StatementNormalizer;
//import com.google.common.collect.ImmutableList;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.util.Optional;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//class ValidatorTest {
//  SqrlParser parser;
//  ErrorCollector errorCollector;
//  SchemaBuilder schema;
//  ImportManager importManager;
//  SchemaUpdatePlanner updatePlanner;
//  StatementNormalizer transpiler;
//
//  @BeforeEach
//  public void setup() throws IOException {
//    errorCollector = ErrorCollector.root();
//
//    SqrlSettings settings = ConfigurationTest.getDefaultSettings(false);
//    Environment env = Environment.create(settings);
//
//    String ds2Name = "ecommerce-data";
//    DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
//        .uri(C360Example.RETAIL_DATA_DIR.toAbsolutePath().toString())
//        .build();
//    env.getDatasetRegistry().addOrUpdateSource(ds2Name, fileConfig, ErrorCollector.root());
//
//    importManager = settings.getImportManagerProvider().createImportManager(env.getDatasetRegistry());
//    ScriptBundle.Config config = ScriptBundle.Config.builder()
//        .name(C360Example.RETAIL_SCRIPT_NAME)
//        .scripts(ImmutableList.of(
//            SqrlScript.Config.builder()
//                .name(C360Example.RETAIL_SCRIPT_NAME)
//                .main(true)
//                .content("IMPORT ecommerce-data.Orders;")
//                .inputSchema(Files.readString(C360Example.RETAIL_IMPORT_SCHEMA_FILE))
//                .build()
//        ))
//        .build();
//    ScriptBundle bundle = config.initialize(errorCollector);
//    System.out.println(errorCollector);
//    importManager.registerUserSchema(bundle.getMainScript().getSchema());
//
//    parser = SqrlParser.newParser(errorCollector);
//    updatePlanner = new SchemaUpdatePlanner(importManager, errorCollector);
//
//    ScriptNode scriptAst = parser.parse(
//        "IMPORT ecommerce-data.Orders;");
//
//    schema = new SchemaBuilder();
//    for (Node node : scriptAst.getStatements()) {
//      Optional<SchemaUpdateOp> operation = updatePlanner.update(schema.getSchema(), node, relNode,
//          relationNorm);
//      operation.ifPresent(schema::apply);
//    }
//
//    transpiler = new StatementNormalizer(importManager, schema.peek());
//  }
//  /**
//   *  Orders.entries.product :=
//   *    JOIN Product ON Product.productid = _.productid;
//   *  Customer.orders :=
//   *    JOIN Orders ON Orders.customerid = _.customerid;
//   *
//   *  Customer.recent_products :=
//   *    SELECT productid, product.category AS category,
//   *         sum(quantity) AS quantity, count(*) AS num_orders
//   *    FROM _.orders.entries
//   *    WHERE parent.`time` > now() - INTERVAL 2 YEAR
//   *    GROUP BY productid, category
//   *    ORDER BY num_orders DESC, quantity DESC;
//   */
//  @Test
//  public void test() {
////    transpile("Orders.copy := SELECT * FROM _.entries ORDER BY quantity + 1;");
////    transpile("Orders.copy := SELECT *, quantity + 1 FROM _.entries ORDER BY quantity + 1;");
////    transpile("Orders.copy := SELECT *, quantity + 1 AS qty FROM _.entries ORDER BY qty;");
////    transpile("Orders.copy := SELECT _._uuid, count() c FROM _.entries e GROUP BY _._uuid HAVING count() + 1 > 4;");
//    transpile("Orders.copy := SELECT e.* FROM _ JOIN _.entries e WHERE _._uuid IS NOT NULL;");
////    transpile("Orders.copy := SELECT count(entries._uuid) FROM _;");
//
//  }
//
//  private void transpile(String sqrl) {
//    System.out.println(sqrl);
//    ScriptNode node = parser.parse(sqrl);
//
////    Relation relation = transpiler.transpile(node.getChildren().get(0));
////    System.out.println(relation.toString());
//
//    SchemaUpdatePlanner schemaUpdatePlanner = new SchemaUpdatePlanner(this.importManager, errorCollector);
//    schemaUpdatePlanner.update(schema.getSchema(), node.getChildren().get(0), relNode, relationNorm);
//
//  }
//
//}