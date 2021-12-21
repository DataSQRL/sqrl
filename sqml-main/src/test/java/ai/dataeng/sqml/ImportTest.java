package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Analyzer;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.env.SqmlEnv;
import ai.dataeng.sqml.execution.SQMLBundle;
import ai.dataeng.sqml.execution.importer.DatasetImportManagerFactory;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.flink.DefaultEnvironmentFactory;
import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.logical4.ImportResolver;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.QueryAnalyzer;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.optimizer.LogicalPlanOptimizer;
import ai.dataeng.sqml.optimizer.SimpleOptimizer;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.physical.flink.FlinkConfiguration;
import ai.dataeng.sqml.physical.flink.FlinkGenerator;
import ai.dataeng.sqml.physical.sql.SQLConfiguration;
import ai.dataeng.sqml.physical.sql.SQLGenerator;
import ai.dataeng.sqml.planner.LogicalPlanBuilder;
import ai.dataeng.sqml.planner.RelationPlan;
import ai.dataeng.sqml.planner.RowNodeIdAllocator;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static ai.dataeng.sqml.physical.sql.SQLConfiguration.Dialect.H2;

class ImportTest {
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



  @Test
  public void test() throws Exception{

    HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
    DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
    SchemaImport schemaImporter = new SchemaImport(ddRegistry, Constraint.FACTORY_LOOKUP);

    ddRegistry.addDataset(new DirectoryDataset(DatasetRegistration.of(RETAIL_DATASET), RETAIL_DATA_DIR));

//    ddRegistry.monitorDatasets(envProvider);

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

    if (schemaImporter.hasErrors()) {
      System.out.println("Import errors:");
      schemaImporter.getErrors().forEach(e -> System.out.println(e));
    }

    System.out.println("-------Import Schema-------");
//    System.out.print(toString(userSchema));

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
    Metadata metadata = new Metadata(new FunctionProvider(),
        env, new DatasetImportManagerFactory(env.getDdRegistry()));

    //Be able to run with a standard import
    SqmlParser parser = SqmlParser.newSqmlParser();

    Script script = parser.parse(
        new String(
            Files.readAllBytes(
                Paths.get(
                "/Users/henneberger/Projects/sqml-official/sqml-examples/retail/c360/c360-small.sqml")))
    );

    //Script processing
    Analysis analysis = Analyzer.analyze(script, metadata);
    System.out.println(analysis.getPlan());

    ///temp import
    LogicalPlan logicalPlan = new LogicalPlan();
    //1. Imports
//    SchemaImport schemaImporter = new SchemaImport(ddRegistry, Constraint.FACTORY_LOOKUP);
//    Map<Name, FlexibleDatasetSchema> userSchema = schemaImporter.convertImportSchema(sqml.parseSchema());
    Preconditions.checkArgument(!schemaImporter.getErrors().isFatal());
    ImportManager sqmlImporter = new ImportManager(ddRegistry);
    sqmlImporter.registerUserSchema(userSchema);

    ConversionError.Bundle<ConversionError> errors2 = new ConversionError.Bundle<>();

    Name ordersName = Name.system("orders");
    ImportResolver importer = new ImportResolver(sqmlImporter, logicalPlan, errors2);
    importer.resolveImport(ImportResolver.ImportMode.TABLE, Name.system(RETAIL_DATASET),
        Optional.of(ordersName), Optional.empty());
    ///


    QueryAssignment node = (QueryAssignment) script.getStatements().get(1);
    StatementAnalysis statementAnalysis = analysis.getStatementAnalysis(node);
    LogicalPlanBuilder planner = new LogicalPlanBuilder(new RowNodeIdAllocator(), metadata, logicalPlan);
    RelationPlan plan = planner.planStatement(statementAnalysis, node.getQuery());
    plan.assemble();
    System.out.println(plan);
    System.out.println();

    QueryAnalyzer.addDevModeQueries(logicalPlan);

    Preconditions.checkArgument(!errors.isFatal());

    LogicalPlanOptimizer.Result optimized = new SimpleOptimizer().optimize(logicalPlan);
    SQLGenerator.Result sql = new SQLGenerator(sqlConfig).generateDatabase(optimized);
    System.out.println(sql);
    final FlinkConfiguration flinkConfig = new FlinkConfiguration(jdbcOptions);

    sql.executeDMLs();
    StreamExecutionEnvironment flinkEnv = new FlinkGenerator(flinkConfig, envProvider).generateStream(optimized, sql.getSinkMapper());
    flinkEnv.execute();


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
//
//    GraphQLSchema graphqlSchema = LogicalGraphqlSchemaBuilder
//        .newGraphqlSchema()
//        .setCodeRegistryBuilder(GraphQLCodeRegistry.newCodeRegistry().build())
//        .analysis(analysis)
//        .build();
//
//    GraphQL graphQL = GraphQL.newGraphQL(graphqlSchema)
//        .build();
//
//    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query("query { orders { customerid, entries { productid } } }")
//        .build();
//    ExecutionResult executionResult = graphQL.execute(executionInput);
//
//    Object data = executionResult.getData();
//    System.out.println(data);
//    List<GraphQLError> errors2 = executionResult.getErrors();
//    System.out.println(errors2);
//    assertEquals(true, errors2.isEmpty());
  }

}