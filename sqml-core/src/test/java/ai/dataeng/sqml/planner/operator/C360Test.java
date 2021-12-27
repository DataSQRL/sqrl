package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptBundle;
import ai.dataeng.sqml.api.graphql.GraphqlSchemaBuilder;
import ai.dataeng.sqml.config.EnvironmentSettings;
import ai.dataeng.sqml.execution.flink.ingest.DatasetRegistration;
import ai.dataeng.sqml.importer.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.planner.Script;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class C360Test {
  public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
  public static final String RETAIL_SCRIPT_NAME = "c360";
  public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
  public static final String SQML_SCRIPT_EXTENSION = ".sqml";
  public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final String RETAIL_DATASET = "ecommerce-data";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);

  Environment env;
  EnvironmentSettings settings;
  @BeforeEach
  public void setup() {
    settings = EnvironmentSettings.createDefault()
        .build();

    env = Environment.create(settings);

    DirectoryDataset dd = new DirectoryDataset(DatasetRegistration.of(RETAIL_DATASET), RETAIL_DATA_DIR);
    env.registerDataset(dd);
  }

  @Test
  @SneakyThrows
  public void testC360() {
    ScriptBundle bundle = new ScriptBundle.Builder().createScript()
        .setName(RETAIL_SCRIPT_NAME)
        .setScript(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION))
        .setImportSchema(RETAIL_IMPORT_SCHEMA_FILE)
        .asMain()
        .add().build();

    Script script = env.compile(bundle);

    GraphQLSchema graphQLSchema = GraphqlSchemaBuilder.newGraphqlSchema()
        .schema(script.getNamespace().getSchema())
        .build();
    System.out.println(new SchemaPrinter().print(graphQLSchema));
  }
}
