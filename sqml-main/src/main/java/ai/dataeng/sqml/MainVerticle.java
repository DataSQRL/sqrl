//package ai.dataeng.sqml;
//
//import graphql.GraphQL;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.handler.graphql.GraphQLHandler;
//import io.vertx.jdbcclient.JDBCConnectOptions;
//import io.vertx.jdbcclient.JDBCPool;
//import io.vertx.sqlclient.PoolOptions;
//import org.dataloader.DataLoader;
//import org.dataloader.DataLoaderRegistry;
//
//public class MainVerticle extends AbstractVerticle {
//
////  private PgPool client;
//  private DataLoader<Integer, Object> characterDataLoader;
//  private JDBCPool client;
//
//  @Override
//  public void start() throws Exception {
//    // Create a Router
//    client = JDBCPool.pool(
//        vertx,
//        // configure the connection
//        new JDBCConnectOptions()
//            // H2 connection string
//            .setJdbcUrl("jdbc:h2:~/test;database_to_upper=false")
//            // username
//            .setUser("sa")
//            // password
//            .setPassword(""),
//        // configure the pool
//        new PoolOptions()
//            .setMaxSize(16)
//    );
//
//    Router router = Router.router(vertx);
//
//    GraphQL graphQL = GraphQL.newGraphQL(null)
//        .build();
//
//    router.route("/graphql").handler(GraphQLHandler.create(graphQL)
//        .dataLoaderRegistry((x)->{
//          DataLoaderRegistry registry =
//              new DataLoaderRegistry();
//          return registry;
//        })
//    );
//
//    // Create the HTTP server
//    vertx.createHttpServer()
//        // Handle every request using the router
//        .requestHandler(router)
//        // Start listening
//        .listen(8888)
//        // Print the port
//        .onSuccess(server ->
//            System.out.println(
//                "HTTP server started on port " + server.actualPort()
//            )
//        );
//  }
//}