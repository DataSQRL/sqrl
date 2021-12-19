package ai.dataeng;

import static ai.dataeng.sqml.physical.sql.SQLConfiguration.Dialect.H2;

import ai.dataeng.execution.criteria.EqualsCriteria;
import ai.dataeng.execution.table.H2Table;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.sqml.analyzer2.Analyzer2;
import ai.dataeng.sqml.analyzer2.GraphqlBuilder;
import ai.dataeng.sqml.analyzer2.LogicalGraphqlSchemaBuilder;
import ai.dataeng.sqml.analyzer2.SqrlSchemaConverter;
import ai.dataeng.sqml.analyzer2.SqrlSinkBuilder;
import ai.dataeng.sqml.analyzer2.TableManager;
import ai.dataeng.sqml.analyzer2.UberTranslator;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.physical.sql.SQLConfiguration;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.name.Name;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

public class SqrlTest {

  @Test
  public void testImport() {
    String script = "IMPORT ecommerce-data.Orders;";
    run(script);
  }

  @Test
  public void testNonDistinct() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Customers := SELECT customerid FROM Orders;";
  }

  @Test
  public void testDistinct() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Customers := SELECT DISTINCT customerid FROM Orders;";
  }

  @Test
  public void testGroupBy() {
    String script = "IMPORT ecommerce-data.Orders\n"
        + "CustomerOrderStats := SELECT customerid, count(1) as num_orders\n"
        + "                      FROM Orders\n"
        + "                      GROUP BY customerid;";
    run(script);
  }
  @Test
  public void testNestedRelation() {
    String script = "IMPORT ecommerce-data.Orders;\n"
        + "Orders.total := SELECT sum(quantity) AS quantity\n"
        + "                FROM @.entries;";
    run(script);
  }

  @Test
  public void testgraphql() throws Exception {
//    GraphqlBuilder.graphqlTest(vertx, schema);
  }


  public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
  //  public static final Path RETAIL_DIR = Path.of(System.getProperty("user.dir")).resolve("sqml-examples").resolve("retail");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final String RETAIL_DATASET = "ecommerce-data";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
  public static final String RETAIL_SCRIPT_NAME = "c360";
  public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
  public static final String SQML_SCRIPT_EXTENSION = ".sqml";
  public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");

//  public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Orders", "Product"};

  public static final Path outputBase = Path.of("tmp","datasource");
  public static final Path dbPath = Path.of("tmp","output");
  //
  private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
      .withDriverName("org.h2.Driver")
      .build();
  private static final SQLConfiguration sqlConfig = new SQLConfiguration(H2,jdbcOptions);

  private static final EnvironmentFactory envProvider = new DefaultEnvironmentFactory();
//  private DataLoader<Integer, Object> characterDataLoader;


  @SneakyThrows
  private static void run(String scriptStr) {
    final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inStreamingMode()
            .build();
    final TableEnvironment env = TableEnvironment.create(settings);

    SqmlParser parser = SqmlParser.newSqmlParser();

    Script script = parser.parse(scriptStr);

    TableManager tableManager = new TableManager();
    //Script processing
    new Analyzer2(script, env, tableManager)
        .analyze();

    LogicalPlan plan = new SqrlSchemaConverter()
        .convert(tableManager);

    VertxOptions vertxOptions = new VertxOptions();
    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);




//    H2Column ordersPk = new UUIDColumn("_uuid_0", "_uuid_0"); //todo: PK identifier
    H2Column columnC = new IntegerColumn("customerid", "customerid");
    H2Column columnid = new IntegerColumn("id", "id");
//    H2Column id = new StringColumn("uuid", "uuid");
    H2Table ordersTable = new H2Table(new Columns(List.of( columnC, columnid)),
        Name.system("Orders").getCanonical().replaceAll("\\.", "_") + "_flink", Optional.empty());

    H2Column column = new IntegerColumn("discount", "discount");
    H2Table entries = new H2Table(new Columns(List.of(column)), "orders_entries_flink",
        Optional.of(new EqualsCriteria("id", "id")));

    H2Table customerOrderStats = new H2Table(new Columns(List.of(
        new IntegerColumn("customerid", "customerid"),
        new IntegerColumn("num_orders", "num_orders")
    )), Name.system("CustomerOrderStats").getCanonical().replaceAll("\\.", "_") + "_flink",
        Optional.empty());

//    Map<String, H2Table> tableMap = Map.of("Orders", ordersTable, "entries", entries, "CustomerOrderStats", customerOrderStats);
//
    Map<String, H2Table> tableMap = new SqrlSinkBuilder(env, tableManager)
        .build(true);

    UberTranslator uberTranslator = new UberTranslator();

    GraphQLSchema schema = new LogicalGraphqlSchemaBuilder(Map.of(), plan.getSchema(), vertx, uberTranslator, tableMap)
        .build();

    System.out.println(new SchemaPrinter().print(schema));

//
    GraphqlBuilder.graphqlTest(vertx, schema);
  }

  @SneakyThrows
  @Test
  public void test() {
  }
}
