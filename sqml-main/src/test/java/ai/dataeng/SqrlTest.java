package ai.dataeng;

import static ai.dataeng.sqml.physical.sql.SQLConfiguration.Dialect.H2;

import ai.dataeng.sqml.analyzer2.Analyzer2;
import ai.dataeng.sqml.analyzer2.GraphqlBuilder;
import ai.dataeng.sqml.analyzer2.SqrlSinkBuilder;
import ai.dataeng.sqml.analyzer2.TableManager;
import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.env.SqmlEnv;
import ai.dataeng.sqml.execution.SQMLBundle;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.physical.sql.SQLConfiguration;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.name.Name;
import java.nio.file.Path;
import java.util.Map;
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
        + "CustomerOrderStats := SELECT customerid, count(*) as num_orders\n"
        + "                      FROM Orders\n"
        + "                      GROUP BY customerid;";
    run(script);
  }

  @Test
  public void testgraphql() throws Exception {
    GraphqlBuilder.graphqlTest();
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

    new SqrlSinkBuilder(env, tableManager)
        .build();

    GraphqlBuilder.graphqlTest();
  }

  @SneakyThrows
  @Test
  public void test() {
  }
}
