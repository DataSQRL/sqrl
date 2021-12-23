package ai.dataeng.sqml;

import ai.dataeng.execution.table.H2Table;
import ai.dataeng.sqml.analyzer2.Analyzer2;
import ai.dataeng.sqml.analyzer2.GraphqlBuilder;
import ai.dataeng.sqml.analyzer2.ImportManager;
import ai.dataeng.sqml.analyzer2.ImportStub;
import ai.dataeng.sqml.analyzer2.LogicalGraphqlSchemaBuilder;
import ai.dataeng.sqml.analyzer2.NameTranslator;
import ai.dataeng.sqml.analyzer2.ScriptProcessor;
import ai.dataeng.sqml.analyzer2.SqrlCatalogManager;
import ai.dataeng.sqml.analyzer2.SqrlEnvironment;
import ai.dataeng.sqml.analyzer2.SqrlSchemaConverter;
import ai.dataeng.sqml.analyzer2.SqrlSchemaResolver;
import ai.dataeng.sqml.analyzer2.SqrlSinkBuilder;
import ai.dataeng.sqml.analyzer2.TableManager;
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
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

public class Sqrl2Test {

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
  public void testQuery() {
    String script = "IMPORT ecommerce-data.Orders\n"
        + "CustomerOrderStats := SELECT customerid, count(1) as num_orders\n"
        + "                      FROM Orders\n"
        + "                      GROUP BY customerid;";
    GraphQL graphQL = run(script);
    query(graphQL, "query {\n"
        + "    customerorderstats { data {customerid, num_orders} }\n"
        + "    orders(limit: 2) {"
        + "        data {"
        + "           customerid, id\n"
        + "        }\n"
        + "    }\n"
        + "}");
  }

  @Test
  public void testExpression() {
    String script = "IMPORT ecommerce-data.Orders\n"
        + "Orders.entries.total := quantity * unit_price - discount;\n";
    GraphQL graphQL = run(script);
    query(graphQL, "query {\n"
        + "    orders {"
        + "        data {"
        + "           entries { total, discount }\n"
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
  public void testNestedQuery() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Orders.ids := SELECT sum(id) AS id2\n"
        + "                FROM Orders;";

    GraphQL graphQL = run(script);

    query(graphQL, "query {\n"
        + "    orders { data { ids { id2 } } } "
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
    String jdbcUrl = postgres.getJdbcUrl();

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

    SqrlCatalogManager sqrlCatalogManager = new SqrlCatalogManager();

    ImportStub importStub = new ImportStub(env, sqrlCatalogManager);

    SqrlEnvironment environment = new SqrlEnvironment(new ImportManager(importStub), env, new SqrlSchemaResolver());
    ScriptProcessor scriptProcessor = new ScriptProcessor(sqrlCatalogManager, environment);
    scriptProcessor.process(script);

    System.out.println(environment);
    LogicalPlan plan = new SqrlSchemaConverter()
        .convert(sqrlCatalogManager);

    VertxOptions vertxOptions = new VertxOptions();
    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);

    Map<String, H2Table> tableMap = new SqrlSinkBuilder(env, jdbcUrl)
        .build(sqrlCatalogManager, true);

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
