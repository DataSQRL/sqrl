//package ai.dataeng.execution;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.fail;
//
//import ai.dataeng.execution.connection.JdbcPool;
//import ai.dataeng.execution.criteria.EqualsCriteria;
//import ai.dataeng.execution.page.NoPage;
//import ai.dataeng.execution.page.SystemPageProvider;
//import ai.dataeng.execution.table.H2Table;
//import ai.dataeng.execution.table.column.Columns;
//import ai.dataeng.execution.table.column.H2Column;
//import ai.dataeng.execution.table.column.IntegerColumn;
//import com.google.common.base.Charsets;
//import graphql.ExecutionInput;
//import graphql.ExecutionResult;
//import graphql.GraphQL;
//import graphql.GraphQLError;
//import graphql.schema.FieldCoordinates;
//import graphql.schema.GraphQLCodeRegistry;
//import graphql.schema.GraphQLSchema;
//import graphql.schema.idl.RuntimeWiring;
//import graphql.schema.idl.SchemaGenerator;
//import graphql.schema.idl.SchemaParser;
//import graphql.schema.idl.TypeDefinitionRegistry;
//import io.vertx.core.Vertx;
//import io.vertx.core.VertxOptions;
//import io.vertx.core.impl.VertxInternal;
//import io.vertx.jdbcclient.JDBCConnectOptions;
//import io.vertx.jdbcclient.JDBCPool;
//import io.vertx.sqlclient.Pool;
//import io.vertx.sqlclient.PoolOptions;
//import java.net.URL;
//import java.util.List;
//import java.util.Optional;
//import lombok.SneakyThrows;
//import org.dataloader.DataLoaderRegistry;
//import org.junit.jupiter.api.Test;
//
////This is a throwaway test for the gql poc
//public class GraphQLTest {
//
//  private GraphQL graphQL;
//  DataLoaderRegistry dataLoaderRegistry;
//
//  @SneakyThrows
//  public void before() {
////
////    PlanItem orders = new PlanItem(ordersField, new Table3("orders", Optional.empty(),
////        List.of("customerid"), List.of(new Column3("customerid_col", customerid, new IntegerColumnType()))));
////
////    PlanItem entries = new PlanItem(entriesField, new Table3("entries", Optional.of(List.of(new ContextKey("customerid_col", "customerid"))),
////        List.of("customerid", "entries_pos"), List.of(
////            new Column3("customerid", customerid, new IntegerColumnType()),
////            new Column3("entries_pos", null, new IntegerColumnType()),
////            new Column3("discount", discount, new FloatColumnType())
////        )));
////
////    PhysicalTablePlan tablePlan = new PhysicalTablePlan(List.of(orders, entries));
////
////    this.tablePlan = tablePlan;
//
//    VertxOptions vertxOptions = new VertxOptions();
//    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);
//
//    //In memory pool, if connection times out then the data is erased
//    Pool client = JDBCPool.pool(
//        vertx,
//        // configure the connection
//        new JDBCConnectOptions()
//            // H2 connection string
//            .setJdbcUrl("jdbc:h2:mem:test;database_to_upper=false")
//            // username
//            .setUser("sa")
//            // password
//            .setPassword(""),
//        // configure the pool
//        new PoolOptions()
//            .setMaxSize(1)
//    );
//
//    //Create tables and add data
//    client.query("create table orders(customerid_col int primary key);")
//        .execute()
//        .onSuccess(e->{
//          System.out.println("Created table.");
//          client.query("insert into orders(customerid_col) values (1), (2)")
//              .execute()
//              .onSuccess((d)->{
//                System.out.println("added fields");
//              })
//              .onFailure(e2->{
//                e2.printStackTrace();
//                fail();
//              });
//        }).onFailure(e->{
//          e.printStackTrace();
//          fail();
//        }).toCompletionStage().toCompletableFuture().get();
//
//    //Create tables and add data
//    client.query("create table entries(customerid int, entries_pos int, discount float);")
//        .execute()
//        .onSuccess(e->{
//          System.out.println("Created table.");
//          client.query("insert into entries values (1, 0, 0.0), (1, 1, 1.0),  (2, 0, 0)")
//              .execute()
//              .onSuccess((d)->{
//                System.out.println("added fields");
//              })
//              .onFailure(e2->{
//                e2.printStackTrace();
//                fail();
//              });
//        }).onFailure(e->{
//          e.printStackTrace();
//          fail();
//        }).toCompletionStage().toCompletableFuture().get();
//
//    SchemaParser schemaParser = new SchemaParser();
//    URL url = com.google.common.io.Resources.getResource("c360.graphqls");
//    String schema = com.google.common.io.Resources.toString(url, Charsets.UTF_8);
//    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);
//
//    SchemaGenerator schemaGenerator = new SchemaGenerator();
//
//    H2Column columnC = new IntegerColumn("customerid", "customerid_col");
//    H2Table orders = new H2Table(new Columns(List.of(columnC)), "orders", Optional.empty());
//
//
//    H2Column column = new IntegerColumn("discount", "discount");
//    H2Table entries = new H2Table(new Columns(List.of(column)), "entries",
//        Optional.of(new EqualsCriteria("customerid", "customerid")));
//
//    dataLoaderRegistry = new DataLoaderRegistry();
//    GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, RuntimeWiring.newRuntimeWiring()
//        .codeRegistry(GraphQLCodeRegistry.newCodeRegistry()
//            .dataFetcher(FieldCoordinates.coordinates("Query", "orders"),
//                new DefaultDataFetcher(new JdbcPool(client), new NoPage(), orders))
//            .dataFetcher(FieldCoordinates.coordinates("orders", "entries"),
//                new DefaultDataFetcher(/*dataLoaderRegistry, */new JdbcPool(client), new SystemPageProvider(), entries))//todo: Data fetcher factory?
//            .build())
//        .build());
//
////    System.out.println(new SchemaPrinter().print(graphQLSchema));
//
//    GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema)
//        .build();
//
//    this.graphQL = graphQL;
//  }
//
//  @Test
//  public void testRootQuery() {
//    before();
//
//    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query("query { orders(filter: {customerid: {equals: 1}}) { customerid } }")
//        .build();
//    ExecutionResult executionResult = graphQL.execute(executionInput);
//
//    Object data = executionResult.getData();
//    System.out.println(data);
//    List<GraphQLError> errors2 = executionResult.getErrors();
//    System.out.println(errors2);
//    assertEquals(true, errors2.isEmpty());
//  }
//
//  @Test
//  public void testNestedQuery() {
//    before();
//    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(
//        "query Test {\n"
//            + "    orders(limit: 2) {\n"
//            + "        customerid"
//            + "        entries("+/*page_size: 1, page_state: \"1\", */"order_by: [{discount: DESC}]) {\n"
//            + "            data {\n"
//            + "                discount\n"
//            + "            } \n"
//            + "            pageInfo { \n"
//            + "                cursor\n"
//            + "                hasNext\n"
//            + "             }\n"
//            + "        }\n"
//            + "    }\n"
//            + "}")
//        .dataLoaderRegistry(dataLoaderRegistry)
//        .build();
//    ExecutionResult executionResult = graphQL.execute(executionInput);
//
//    Object data = executionResult.getData();
//    System.out.println(data);
//    List<GraphQLError> errors2 = executionResult.getErrors();
//    System.out.println(errors2);
//    assertEquals(true, errors2.isEmpty());
//  }
//
//  @Test
//  public void testArguments() {
//
//  }
//
//  @Test
//  public void testLimit() {
//
//  }
//
//  @Test
//  public void testPaging() {
//
//  }
//}