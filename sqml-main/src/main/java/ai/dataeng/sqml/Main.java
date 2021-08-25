package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Analyzer;
import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.db.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.db.tabular.JDBCSinkFactory;
import ai.dataeng.sqml.db.tabular.RowMapFunction;
import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.flink.DefaultEnvironmentProvider;
import ai.dataeng.sqml.flink.EnvironmentProvider;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.function.PostgresFunctions;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.Metadata.TableHandle;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.schema.SchemaProvider;
import ai.dataeng.sqml.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import graphql.schema.GraphQLSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.jooq.SQLDialect;

public class Main {

  public static void main(String[] args) throws Exception {
    Bundle bundle = Bundle.bundle()
        .setPath(Paths.get("sqml-examples/retail"))
        .setMainScript("sqml-examples/retail/c360/c360.sqml")
        .build();

    Main main = new Main();
    main.startBundle(bundle);
  }
  public static final Path dbPath = Path.of("tmp","output");

  public static final Path RETAIL_DIR = Path.of(System.getProperty("user.dir")).resolve("sqml-examples").resolve("retail");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);
  public static final Path outputBase = Path.of("tmp","datasource");

  private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
      .withDriverName("org.h2.Driver")
      .build();


  private void startBundle(Bundle bundle) throws Exception {
    SqmlParser parser = SqmlParser.newSqmlParser();
    Script script = parser.parse(new String(
        Files.readAllBytes(Paths.get(bundle.getMainScriptName()))));

    final EnvironmentProvider envProvider = new DefaultEnvironmentProvider();

    Metadata metadata = new Metadata(FunctionProvider.newFunctionProvider()
        .function(PostgresFunctions.SqmlSystemFunctions).build(), null,
        null, null, bundle, new SchemaProvider(null),
        createDatasetRegistry(), envProvider.get(),
        StreamTableEnvironment.create(envProvider.get())
      );

    ImportPipelineResolver importPipeline = new ImportPipelineResolver(metadata);
    importPipeline.analyze(script);

    Analysis analysis = Analyzer.analyze(script, metadata);

    System.out.println(analysis);

    FlinkViewBuilder viewBuilder = new FlinkViewBuilder(analysis, metadata);
    viewBuilder.build();

    GraphQLSchema graphqlSchema = GraphqlSchemaBuilder
        .newGraphqlSchema()
        .script(script)
        .analysis(analysis)
        .build();

    JDBCSinkFactory dbSinkFactory = new JDBCSinkFactory(jdbcOptions, SQLDialect.H2);

    for(Map.Entry<QualifiedName, TableHandle> table : metadata.getTableHandles().entrySet()) {
      metadata.getStreamTableEnvironment().toRetractStream(table.getValue().getTable(), Row.class).flatMap(new RowMapFunction())
          .addSink(dbSinkFactory.getSink(String.join("_", table.getKey().getParts()), table.getValue().getSchema()));
    }
    metadata.getFlinkEnv().execute();
  }

  private DataSourceRegistry createDatasetRegistry() {
    HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
    DataSourceRegistry ddRegistry = new DataSourceRegistry(kvStoreFactory);
    DirectoryDataset dd = new DirectoryDataset(RETAIL_DATA_DIR);
    ddRegistry.addDataset(dd);
    return ddRegistry;
  }
}
