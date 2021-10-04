package ai.dataeng.sqml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Analyzer;
import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.env.SqmlEnv;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.function.PostgresFunctions;
import ai.dataeng.sqml.imports.ImportLoader;
import ai.dataeng.sqml.imports.TableImportObject;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.physical.PhysicalPlan;
import ai.dataeng.sqml.physical.PhysicalPlanner;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.tree.Script;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.junit.jupiter.api.Test;

class ImportTest {
  public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
  public static final String RETAIL_SCRIPT_NAME = "c360";
  public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
  public static final String SQML_SCRIPT_EXTENSION = ".sqml";

  public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Orders", "Product"};

  public static final Path outputBase = Path.of("tmp","datasource");
  public static final Path dbPath = Path.of("tmp","output");

  private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
      .withDriverName("org.h2.Driver")
      .build();

  @Test
  public void test() throws Exception {
    HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
    DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
    DirectoryDataset dd = new DirectoryDataset(null, RETAIL_DATA_DIR);
    ddRegistry.addDataset(dd);

    ImportLoader importLoader = new ImportLoader();
    for (SourceTable table : dd.getTables()) {
      String path = RETAIL_DATA_DIR_NAME + "." + table.getName();
      importLoader.register(path,
          new TableImportObject(path));
    }

    SqmlEnv env = new SqmlEnv(ddRegistry);

    //The Data needed for sqml
    Metadata metadata = new Metadata(new FunctionProvider(PostgresFunctions.SqmlSystemFunctions),
        env, importLoader);

    //Be able to run with a standard import
    SqmlParser parser = SqmlParser.newSqmlParser();

    Script script = parser.parse(
        "IMPORT ecommerce-data.*;\n"
//        + "IMPORT ai.dataeng.sqml.functions.Echo;"
//        + "IMPORT ai.dataeng.sqml.functions.EchoAgg;"
        + "Product := DISTINCT Product ON (productid);\n"
        + "Product.nested := SELECT * FROM @ limit 10;\n"
    );

    //Script processing
    Analysis analysis = Analyzer.analyze(script, metadata);

    analysis.getDefinedFunctions().stream()
        .forEach((f)->{
          try {
            if (f.isAggregation()) {
              env.getConnectionProvider().getOrEstablishConnection()
                  .createStatement().execute(String.format("DROP AGGREGATE IF EXISTS %s;", f.getName()));
              env.getConnectionProvider().getOrEstablishConnection()
                  .createStatement().execute(String.format("CREATE AGGREGATE %s FOR \"%s\";", f.getName(),
                      f.getClass().getName()));

            } else {
              env.getConnectionProvider().getOrEstablishConnection()
                  .createStatement()
                  .execute(String.format("DROP ALIAS IF EXISTS %s ", f.getName()));

              String registerFunctionQuery = String.format("CREATE OR REPLACE ALIAS %s \n"
                  + "   FOR \"%s\";", f.getName(), f.getClass().getName() + ".fn");

              env.getConnectionProvider().getOrEstablishConnection()
                  .createStatement().execute(registerFunctionQuery);
            }

          } catch (SQLException e) {
            e.printStackTrace();
          } catch (ClassNotFoundException e) {
            e.printStackTrace();
          }
        });

    LogicalPlan logicalPlan = analysis.getLogicalPlan();

    PhysicalPlanner planner = new PhysicalPlanner(env, analysis, metadata);
    logicalPlan.accept(planner, null);
    PhysicalPlan physicalPlan = planner.getPhysicalPlan();

//    env.getFlinkEnv().execute();

//    CodeRegistryBuilder codeRegistryBuilder = new CodeRegistryBuilder();
//    physicalPlan.accept(codeRegistryBuilder, null);

    GraphQLSchema graphqlSchema = LogicalGraphqlSchemaBuilder
        .newGraphqlSchema()
        .analysis(analysis)
        .physicalPlan(physicalPlan)
//        .codeRegistry(codeRegistryBuilder.build())
        .build();

    GraphQL graphQL = GraphQL.newGraphQL(graphqlSchema)
        .build();

    GraphqlSqmlContext context = new GraphqlSqmlContext(env);

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query("query { product { name, nested {name} } }")
        .context(context)
        .build();

    ExecutionResult executionResult = graphQL.execute(executionInput);

    Object data = executionResult.getData();
    System.out.println(data);
    List<GraphQLError> errors = executionResult.getErrors();
    System.out.println(errors);
    assertEquals(true, errors.isEmpty());
  }
}