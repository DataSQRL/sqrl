package ai.dataeng.sqml.analyzer2;

import ai.dataeng.execution.DefaultDataFetcher;
import ai.dataeng.execution.connection.JdbcPool;
import ai.dataeng.execution.criteria.EqualsCriteria;
import ai.dataeng.execution.page.NoPage;
import ai.dataeng.execution.page.SystemPageProvider;
import ai.dataeng.execution.table.H2Table;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.StringColumn;
import ai.dataeng.execution.table.column.UUIDColumn;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Charsets;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import org.dataloader.DataLoaderRegistry;

public class GraphqlBuilder {


  public static void graphqlTest() throws Exception {

    ///Graphql bit

    SchemaParser schemaParser = new SchemaParser();
    URL url = com.google.common.io.Resources.getResource("c360-small.graphqls");
    String schema = com.google.common.io.Resources.toString(url, Charsets.UTF_8);
    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

    graphql.schema.idl.SchemaGenerator schemaGenerator = new graphql.schema.idl.SchemaGenerator();

    VertxOptions vertxOptions = new VertxOptions();
    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);

    //In memory pool, if connection times out then the data is erased
    Pool client = JDBCPool.pool(
        vertx,
        // configure the connection
        new JDBCConnectOptions()
            // H2 connection string
            .setJdbcUrl("jdbc:postgresql://localhost:5432/henneberger")
        // username
//                .setUser("sa")
        // password
//                .setPassword("")
        ,
        // configure the pool
        new PoolOptions()
            .setMaxSize(1)
    );

//    H2Column ordersPk = new UUIDColumn("_uuid_0", "_uuid_0"); //todo: PK identifier
    H2Column columnC = new IntegerColumn("customerid", "customerid");
    H2Column columnid = new IntegerColumn("id", "id");
//    H2Column id = new StringColumn("uuid", "uuid");
    H2Table ordersTable = new H2Table(new Columns(List.of( columnC, columnid)),
        Name.system("Orders").getCanonical().replaceAll("\\.", "_") + "_flink", Optional.empty());

    H2Column column = new IntegerColumn("discount", "discount");
    H2Table entries = new H2Table(new Columns(List.of(column)), "orders_entries_flink",
        Optional.of(new EqualsCriteria("id", "id")));
//
    H2Table customerOrderStats = new H2Table(new Columns(List.of(
        new IntegerColumn("customerid", "customerid"),
        new IntegerColumn("num_orders", "num_orders")
    )), Name.system("CustomerOrderStats").getCanonical().replaceAll("\\.", "_") + "_flink",
        Optional.empty());

    DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();
    GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, RuntimeWiring.newRuntimeWiring()
        .codeRegistry(GraphQLCodeRegistry.newCodeRegistry()
            .dataFetcher(FieldCoordinates.coordinates("Query", "orders"),
                new DefaultDataFetcher(new JdbcPool(client), new NoPage(), ordersTable))
            .dataFetcher(FieldCoordinates.coordinates("orders", "entries"),
                new DefaultDataFetcher(/*dataLoaderRegistry, */new JdbcPool(client), new SystemPageProvider(), entries))
            .dataFetcher(FieldCoordinates.coordinates("Query", "CustomerOrderStats"),
                new DefaultDataFetcher(new JdbcPool(client), new NoPage(), customerOrderStats))
            .build())
        .build());

//    System.out.println(new SchemaPrinter().print(graphQLSchema));

    GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema)
        .build();

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(
            "query Test {\n"
                + "    CustomerOrderStats { customerid, num_orders }\n"
                + "    orders(limit: 2) {\n"
                + "        customerid, id\n"
                + "        entries(order_by: [{discount: DESC}]) {\n"
                + "            data {\n"
                + "                discount\n"
                + "            } \n"
                + "            pageInfo { \n"
                + "                cursor\n"
                + "                hasNext\n"
                + "             }\n"
                + "        }\n"
                + "    }\n"
                + "}")
        .dataLoaderRegistry(dataLoaderRegistry)
        .build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Object data = executionResult.getData();
    System.out.println();
    System.out.println(data);
    List<GraphQLError> errors2 = executionResult.getErrors();
    System.out.println(errors2);

    vertx.close();
  }
}
