//package ai.dataeng.sqml;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.fail;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import ai.dataeng.sqml.parser.validator.script.analyzer.Analysis;
//import ai.dataeng.sqml.parser.validator.script.analyzer.Analyzer;
//import ai.dataeng.sqml.execution.importer.ImportManager;
//import ai.dataeng.sqml.imports.ImportManagerFactory;
//import ai.dataeng.sqml.imports.ImportSchema;
//import ai.dataeng.sqml.imports.ImportSchema.ImportType;
//import ai.dataeng.sqml.imports.ImportSchema.Mapping;
//import ai.dataeng.sqml.function.FunctionProvider;
//import ai.dataeng.sqml.function.PostgresFunctions;
//import ai.dataeng.sqml.graphql.LogicalGraphqlSchemaBuilder;
//import ai.dataeng.sqml.metadata.Metadata;
//import ai.dataeng.sqml.parser.SqmlParser;
//import ai.dataeng.sqml.schema2.ArrayType;
//import ai.dataeng.sqml.schema2.RelationType;
//import ai.dataeng.sqml.schema2.StandardField;
//import ai.dataeng.sqml.schema2.TypedField;
//import ai.dataeng.sqml.schema2.basic.FloatType;
//import ai.dataeng.sqml.schema2.basic.IntegerType;
//import ai.dataeng.sqml.tree.Script;
//import ai.dataeng.sqml.tree.name.Name;
//import graphql.ExecutionInput;
//import graphql.ExecutionResult;
//import graphql.GraphQL;
//import graphql.GraphQLError;
//import graphql.schema.GraphQLCodeRegistry;
//import graphql.schema.GraphQLSchema;
//import graphql.schema.idl.SchemaPrinter;
//import io.vertx.core.Vertx;
//import io.vertx.core.VertxOptions;
//import io.vertx.core.impl.VertxInternal;
//import io.vertx.jdbcclient.JDBCConnectOptions;
//import io.vertx.jdbcclient.JDBCPool;
//import io.vertx.sqlclient.Pool;
//import io.vertx.sqlclient.PoolOptions;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import lombok.SneakyThrows;
//import org.h2.table.PlanItem;
//import org.junit.Before;
//import org.junit.Test;
//
////This is a throwaway test for the gql poc
//public class GraphQLTest {
//
//  private Analysis analysis;
//  private PhysicalTablePlan tablePlan;
//  private GraphQL graphQL;
//  private CodeRegistryBuilder3 codeRegistryBuilder3;
//
//  @SneakyThrows
//  @Before
//  public void test() {
//    SqmlParser parser = SqmlParser.newSqmlParser();
//
//    Script script = parser.parse("IMPORT ecommerce.Orders;");
//
//    RelationType ordersRel = new RelationType<>();
//    StandardField customerid = new StandardField(Name.system("customerid"), new IntegerType(), List.of(), Optional.empty());
//    ordersRel.add(customerid);
//    TypedField ordersField = new StandardField(Name.system("orders"), new ArrayType(ordersRel),
//        List.of(), Optional.empty());
//
//    RelationType entriesRel = new RelationType();
//    StandardField entriesField = new StandardField(Name.system("entries"), new ArrayType(entriesRel), List.of(), Optional.empty());
//    ordersRel.add(entriesField);
//
//    StandardField discount = new StandardField(Name.system("discount"), new FloatType(), List.of(), Optional.empty());
//    entriesRel.add(discount);
//
//    RelationType root = new RelationType();
//    root.add(ordersField);
//
//    Metadata metadata = new Metadata(new FunctionProvider(PostgresFunctions.SqmlSystemFunctions),
//        null, null, new ImportManagerFactory() {
//      @Override
//      public ImportManager create() {
//        ImportManager importManager = mock(ImportManager.class);
//        when(importManager.createImportSchema(any())).thenReturn(
//            new ImportSchema(null, null, root, Map.of(Name.system("orders"),
//                new Mapping(ImportType.SOURCE, Name.system("orders"), Name.system("orders"))))
//        );
//        return importManager;
//      }
//    });
//    Analysis analysis = Analyzer.analyze(script, metadata);
//    System.out.println(analysis.getPlan());
//
//    this.analysis = analysis;
//
//    PlanItem orders = new PlanItem(ordersField, new Table3("orders", Optional.empty(),
//        List.of("customerid"), List.of(new Column3("customerid_col", customerid, new IntegerColumnType()))));
//
//    PlanItem entries = new PlanItem(entriesField, new Table3("entries", Optional.of(List.of(new ContextKey("customerid_col", "customerid"))),
//        List.of("customerid", "entries_pos"), List.of(
//            new Column3("customerid", customerid, new IntegerColumnType()),
//            new Column3("entries_pos", null, new IntegerColumnType()),
//            new Column3("discount", discount, new FloatColumnType())
//        )));
//
//    PhysicalTablePlan tablePlan = new PhysicalTablePlan(List.of(orders, entries));
//
//    this.tablePlan = tablePlan;
//    //Create h2 table so its actually executable (?)
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
//
//
//    codeRegistryBuilder3 = new CodeRegistryBuilder3(client);
//    GraphQLCodeRegistry registry = codeRegistryBuilder3.build(tablePlan);
//
//    GraphQLSchema graphqlSchema = LogicalGraphqlSchemaBuilder
//        .newGraphqlSchema()
//        .setCodeRegistryBuilder(registry)
//        .analysis(analysis)
//        .build();
//
//    System.out.println(new SchemaPrinter().print(graphqlSchema));
//
//    GraphQL graphQL = GraphQL.newGraphQL(graphqlSchema)
//        .build();
//
//    this.graphQL = graphQL;
//  }
//
//  @Test
//  public void testRootQuery() {
//    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query("query { orders { customerid } }")
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
//    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query("query { orders { customerid, entries { discount } } }")
//        .dataLoaderRegistry(codeRegistryBuilder3.getDataLoaderRegistry())
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