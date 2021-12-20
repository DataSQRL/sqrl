package ai.dataeng;

import static org.testcontainers.containers.PostgreSQLContainer.DEFAULT_TAG;

import ai.dataeng.execution.table.H2Table;
import ai.dataeng.sqml.analyzer2.Analyzer2;
import ai.dataeng.sqml.analyzer2.GraphqlBuilder;
import ai.dataeng.sqml.analyzer2.ImportStub;
import ai.dataeng.sqml.analyzer2.LogicalGraphqlSchemaBuilder;
import ai.dataeng.sqml.analyzer2.SqrlSchemaConverter;
import ai.dataeng.sqml.analyzer2.SqrlSinkBuilder;
import ai.dataeng.sqml.analyzer2.TableManager;
import ai.dataeng.sqml.analyzer2.NameTranslator;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.tree.Script;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

public class SqrlTest {

  @Rule
  public PostgreSQLContainer postgres = new PostgreSQLContainer();

  @Test
  public void testImport() {
    String script = "IMPORT ecommerce-data.Orders;";
    GraphQL graphQL = run(script);
    query(graphQL, "query {\n"
        + "    orders {\n"
        + "        data {\n"
        + "           id\n"
        + "           customerid\n"
        + "           time\n"
        + "           entries {\n"
        + "             productid\n"
        + "             quantity\n"
        + "             unit_price\n"
        + "             discount\n"
        + "           }\n"
        + "        }\n"
        + "    }\n"
        + "}");
  }

  @Test
  public void testDistinct() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Customers := SELECT DISTINCT customerid FROM Orders;";
    GraphQL graphQL = run(script);
    query(graphQL, "query {\n"
        + "    customers { data { customerid } }\n"
        + "}");
  }

  @Test
  public void testGroupBy() {
    String script = "IMPORT ecommerce-data.Orders\n"
        + "Orders.entries.total := quantity * unit_price - discount;\n"
        + "CustomerOrderStats := SELECT customerid, count(1) as num_orders\n"
        + "                      FROM Orders\n"
        + "                      GROUP BY customerid;";
    GraphQL graphQL = run(script);
    query(graphQL, "query {\n"
        + "    customerorderstats { data {customerid, num_orders} }\n"
        + "    orders(limit: 2) {"
        + "        data {"
        + "           customerid, id\n"
        + "           entries (filter: {total: {gt: 100}}, order: [{discount: DESC}]){ total, discount }\n"
        + "        }\n"
        + "    }\n"
        + "}");
  }

  @Test
  public void testNoAgg() {
    String script = "IMPORT ecommerce-data.Orders\n"
        + "Customers := SELECT customerid\n"
        + "             FROM Orders;";

    GraphQL graphQL = run(script);

    query(graphQL,"query Test {\n"
                + "    customers { data { customerid } }\n"
                + "}");
  }

  @Test
  public void testNestedRelation() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Orders.total := SELECT sum(quantity) AS quantity\n"
        + "                FROM @.entries;";

    GraphQL graphQL = run(script);

    query(graphQL, "query {\n"
        + "    orders { data { total { quantity }} }\n"
        + "}");
  }

  @Test
  public void testDevMode() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Orders.total := SELECT sum(quantity) AS quantity\n"
        + "                FROM @.entries;"
        + "Orders.total2 := SELECT DISTINCT quantity\n"
        + "                FROM @.total;"
        + "Orders.total3 := SELECT quantity\n"
        + "                FROM @.total;";

    GraphQL graphQL = run(script, true);

    query(graphQL, "query {\n"
        + "    orders { data { total { quantity } total2 { quantity } total2 { quantity } total3 { quantity }}  }\n"
        + "}");
  }

  private GraphQL run(String scriptStr) {
    return run(scriptStr, false);
  }

  @SneakyThrows
  private GraphQL run(String scriptStr, boolean devMode) {
    String jdbcUrl = postgres
        .getJdbcUrl();
//    String jdbcUrl = "";
    final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inStreamingMode()
            .build();
    final TableEnvironment env = TableEnvironment.create(settings);
    env.getConfig().addConfiguration(
        new Configuration()
            .set(RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED(), true)
    );


    SqmlParser parser = SqmlParser.newSqmlParser();

    Script script = parser.parse(scriptStr);

    TableManager tableManager = new TableManager();

    new Analyzer2(script, env, tableManager, new ImportStub(env, tableManager), devMode)
        .analyze();

    LogicalPlan plan = new SqrlSchemaConverter()
        .convert(tableManager);

    VertxOptions vertxOptions = new VertxOptions();
    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);

    Map<String, H2Table> tableMap = new SqrlSinkBuilder(env, tableManager, jdbcUrl)
        .build(true);

    NameTranslator nameTranslator = new NameTranslator();

    //In memory pool, if connection times out then the data is erased
    Pool client = JDBCPool.pool(
        vertx,
        // configure the connection
        new JDBCConnectOptions()
            .setJdbcUrl(jdbcUrl)
            .setUser("test")
            .setPassword("test"),
        // configure the pool
        new PoolOptions()
    );

    GraphQLSchema schema = new LogicalGraphqlSchemaBuilder(Map.of(), plan.getSchema(), vertx,
        nameTranslator, tableMap, client)
        .build();

    System.out.println(new SchemaPrinter().print(schema));

    GraphQL graphQL = GraphqlBuilder.graphqlTest(vertx, schema);

    return graphQL;
  }

  private void query(GraphQL graphQL, String query) {

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query)
//        .dataLoaderRegistry(dataLoaderRegistry)
        .build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Object data = executionResult.getData();
    System.out.println();
    System.out.println(data);
    List<GraphQLError> errors2 = executionResult.getErrors();
    System.out.println(errors2);
    if (!errors2.isEmpty()) {
      throw new RuntimeException(errors2.toString());
    }
  }
}
