//package ai.dataeng.sqml.servlet;
//
//import ai.dataeng.execution.table.H2Table;
//import ai.dataeng.sqml.analyzer2.Analyzer2;
//import ai.dataeng.sqml.analyzer2.GraphqlBuilder;
//import ai.dataeng.sqml.analyzer2.ImportStub;
//import ai.dataeng.sqml.analyzer2.LogicalGraphqlSchemaBuilder;
//import ai.dataeng.sqml.analyzer2.SqrlSchemaConverter;
//import ai.dataeng.sqml.analyzer2.SqrlSinkBuilder;
//import ai.dataeng.sqml.analyzer2.TableManager;
//import ai.dataeng.sqml.analyzer2.NameTranslator;
//import ai.dataeng.sqml.logical4.LogicalPlan;
//import ai.dataeng.sqml.parser.SqmlParser;
//import ai.dataeng.sqml.tree.Script;
//import com.fasterxml.jackson.module.blackbird.BlackbirdModule;
//import graphql.GraphQL;
//import graphql.schema.GraphQLSchema;
//import graphql.schema.idl.SchemaPrinter;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Vertx;
//import io.vertx.core.json.jackson.DatabindCodec;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.handler.BodyHandler;
//import io.vertx.ext.web.handler.graphql.GraphQLHandler;
//import java.util.Map;
//import lombok.SneakyThrows;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.TableEnvironment;
//
//public class Servlet extends AbstractVerticle {
////
////  private final GraphQL graphQL;
////
////  public Servlet(GraphQL graphQL) {
////    this.graphQL = graphQL;
////  }
//  static {
//    DatabindCodec.mapper().registerModule(new BlackbirdModule());
//    DatabindCodec.prettyMapper().registerModule(new BlackbirdModule());
//  }
//
//  private static final String SERVER = "vertx-web";
//  private String date;
//
//  public static void main(String[] args) {
//    Vertx vertx = Vertx.vertx();
//    vertx.deployVerticle(new Servlet());
//  }
//  @SneakyThrows
//  @Override
//  public void start() {
//    final EnvironmentSettings settings =
//        EnvironmentSettings.newInstance().inStreamingMode()
//            .build();
//    final TableEnvironment env = TableEnvironment.create(settings);
//
//    SqmlParser parser = SqmlParser.newSqmlParser();
//
//    Script script = parser.parse("IMPORT ecommerce-data.Orders\n"
//        + "Orders.entries.total := quantity * unit_price - discount;\n"
//        + "CustomerOrderStats := SELECT customerid, count(1) as num_orders\n"
//        + "                      FROM Orders\n"
//        + "                      GROUP BY customerid;");
//
//    TableManager tableManager = new TableManager();
//    //Script processing
//    new Analyzer2(script, env, tableManager, new ImportStub(env, tableManager, null), false)
//        .analyze();
//
//    LogicalPlan plan = new SqrlSchemaConverter()
//        .convert(tableManager);
//
////    VertxOptions vertxOptions = new VertxOptions();
////    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);
//
//    //TODO: create jdbc
//    Map<String, H2Table> tableMap = new SqrlSinkBuilder(env, "")
//        .build(null, true);
//
//    NameTranslator nameTranslator = new NameTranslator();
//
//    GraphQLSchema schema = new LogicalGraphqlSchemaBuilder(Map.of(), plan.getSchema(), vertx,
//        nameTranslator, tableMap, /*Todo: pool*/null)
//        .build();
//
//    System.out.println(new SchemaPrinter().print(schema));
//
////
//    GraphQL graphQL = GraphqlBuilder.graphqlTest(vertx, schema);
//
//
//
////    final Router router = Router.router(vertx);
////    router.route("/graphql").handler(GraphQLHandler.create(graphQL));
//
////
////    router.get("/plaintext").handler(ctx -> {
////      ctx.response()
////          .putHeader(HttpHeaders.SERVER, SERVER)
////          .putHeader(HttpHeaders.DATE, date)
////          .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
////          .end("Hello, World!");
////    });
//    Router router = Router.router(vertx);
//    router.route().handler(BodyHandler.create()); // (2)
//    router.route("/graphql").handler(GraphQLHandler.create(graphQL)); // (3)
//
//    vertx.createHttpServer()
//        .requestHandler(router)
//        .listen(8080);
////
////    vertx.createHttpServer().requestHandler(router).listen(8080, listen -> {
////      if (listen.failed()) {
////        listen.cause().printStackTrace();
////        System.exit(1);
////      }
////    });
//  }
//}
//
///*
//curl -g \
//    -X POST \
//    -H "Content-Type: application/json" \
//    -d '{"query":"query{orders{customerid}}"}' \
//    http://localhost:8080/graphql
//
// */