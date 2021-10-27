package ai.dataeng.sqml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Analyzer;
import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.env.SqmlEnv;
import ai.dataeng.sqml.execution.SQMLBundle;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.function.PostgresFunctions;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.tree.Script;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.junit.jupiter.api.Test;

class ImportTest {
  public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
//  public static final Path RETAIL_DIR = Path.of(System.getProperty("user.dir")).resolve("sqml-examples").resolve("retail");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final String RETAIL_DATASET = "ecommerce";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
  public static final String RETAIL_SCRIPT_NAME = "c360";
  public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
  public static final String SQML_SCRIPT_EXTENSION = ".sqml";
  public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");

  public static final String[] RETAIL_TABLE_NAMES = { "Customer", "Orders", "Product"};

  public static final Path outputBase = Path.of("tmp","datasource");
  public static final Path dbPath = Path.of("tmp","output");

  private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
      .withDriverName("org.h2.Driver")
      .build();
  private static final EnvironmentFactory envProvider = new DefaultEnvironmentFactory();


  @Test
  public void test() throws Exception{

    HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
    DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
    SchemaImport schemaImporter = new SchemaImport(ddRegistry, Constraint.FACTORY_LOOKUP);

    ddRegistry.addDataset(new DirectoryDataset(DatasetRegistration.of(RETAIL_DATASET), RETAIL_DATA_DIR));
//
//    ddRegistry.monitorDatasets(envProvider);
//
//    Thread.sleep(1000);

    SQMLBundle bundle = new SQMLBundle.Builder().createScript().setName(RETAIL_SCRIPT_NAME)
        .setScript(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION))
        .setImportSchema(RETAIL_IMPORT_SCHEMA_FILE)
        .asMain()
        .add().build();
//
    SQMLBundle.SQMLScript sqml = bundle.getMainScript();
//
    Map<Name, FlexibleDatasetSchema> userSchema = schemaImporter.convertImportSchema(sqml.parseSchema());
//
//    if (schemaImporter.hasErrors()) {
//      System.out.println("Import errors:");
//      schemaImporter.getErrors().forEach(e -> System.out.println(e));
//    }
//
//    System.out.println("-------Import Schema-------");
////    System.out.print(toString(userSchema));
//
    ImportManager sqmlManager = new ImportManager(ddRegistry);
    sqmlManager.registerUserSchema(userSchema);

    sqmlManager.importAllTable(RETAIL_DATASET);

    ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
    ImportSchema schema = sqmlManager.createImportSchema(errors);

    if (errors.hasErrors()) {
      System.out.println("-------Import Errors-------");
      errors.forEach(e -> System.out.println(e));
    }

    System.out.println("-------Script Schema-------");
    System.out.println(schema.getSchema());
    System.out.println("-------Tables-------");

//    ImportLoader importLoader = new ImportLoader(sqmlManager, schema);
//    for (Name name : schema.getMappings().keySet()) {
//      importLoader.register(RETAIL_DATA_DIR_NAME + "." + name.getCanonical(), new TableImportObject(
//          schema.getMappings().get(name), RETAIL_DATA_DIR_NAME + "." + name.getCanonical()
//      ));
//    }

    SqmlEnv env = new SqmlEnv(ddRegistry);

    //The Data needed for sqml
    Metadata metadata = new Metadata(new FunctionProvider(PostgresFunctions.SqmlSystemFunctions),
        env, null);

    //Be able to run with a standard import
    SqmlParser parser = SqmlParser.newSqmlParser();

    Script script = parser.parse(
        new String(
            Files.readAllBytes(
                Paths.get(
                "/Users/henneberger/Projects/sqml-official/sqml-examples/retail/c360/c360.sqml")))
//
//        "IMPORT ecommerce.Product;\n"
//        + "IMPORT ai.dataeng.sqml.functions.Echo;"
//        + "IMPORT ai.dataeng.sqml.functions.EchoAgg;"
//        + "Product := DISTINCT Product ON (productid);\n"
//        + "Product.nested := SELECT distinct name FROM Product GROUP BY name;\n"
//        + "Product.nested2 := SELECT name, count(1) FROM parent GROUP BY name;\n" //no name
//        + "Product.echo := echo(name);\n"
//        + "Product.echoAgg := echoagg(name);\n"
//        + "Product.nested.nested := SELECT name, count(1) as cnt FROM @ GROUP BY name;\n" //nested on @
//        + "Product.nested.nested := SELECT name, count(1) as cnt FROM @ GROUP BY name;\n" //nested on @

    );

    //Script processing
    Analysis analysis = Analyzer.analyze(script, metadata);
    System.out.println(analysis.getPlan());
//
//    analysis.getDefinedFunctions().stream()
//        .forEach((f)->{
//          try {
//            if (f.isAggregation()) {
//              env.getConnectionProvider().getOrEstablishConnection()
//                  .createStatement().execute(String.format("DROP AGGREGATE IF EXISTS %s;", f.getName()));
//              env.getConnectionProvider().getOrEstablishConnection()
//                  .createStatement().execute(String.format("CREATE AGGREGATE %s FOR \"%s\";", f.getName(),
//                      f.getClass().getName()));
//
//            } else {
//              env.getConnectionProvider().getOrEstablishConnection()
//                  .createStatement()
//                  .execute(String.format("DROP ALIAS IF EXISTS %s ", f.getName()));
//
//              String registerFunctionQuery = String.format("CREATE OR REPLACE ALIAS %s \n"
//                  + "   FOR \"%s\";", f.getName(), f.getClass().getName() + ".fn");
//
//              env.getConnectionProvider().getOrEstablishConnection()
//                  .createStatement().execute(registerFunctionQuery);
//            }
//
//          } catch (SQLException e) {
//            e.printStackTrace();
//          } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//          }
//        });

    GraphQLSchema graphqlSchema = LogicalGraphqlSchemaBuilder
        .newGraphqlSchema()
        .analysis(analysis)
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
    List<GraphQLError> errors2 = executionResult.getErrors();
    System.out.println(errors2);
    assertEquals(true, errors2.isEmpty());
  }
}