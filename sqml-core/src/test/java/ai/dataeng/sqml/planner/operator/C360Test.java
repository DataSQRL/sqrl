package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.EnvironmentSettings;
import ai.dataeng.sqml.ScriptBundle;
import ai.dataeng.sqml.config.provider.ImportProcessorProvider;
import ai.dataeng.sqml.parser.processor.ImportProcessor;
import ai.dataeng.sqml.tree.ImportDefinition;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class C360Test {
  public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
  public static final String RETAIL_SCRIPT_NAME = "c360";
  public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
  public static final String SQML_SCRIPT_EXTENSION = ".sqml";
  public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");

  Environment env;

  @BeforeEach
  public void setup() {
    EnvironmentSettings settings = EnvironmentSettings.createDefault()
        .importProcessorProvider((datasetManager -> new StubImportProcessor()))
        .build();

    env = Environment.create(settings);
  }

  @AfterEach
  public void tearDown() {
    try {
      env.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SneakyThrows
  public void run(String name, String script) {
    ScriptBundle bundle = new ScriptBundle.Builder().createScript()
        .setName(name)
        .setScript(script)
        .setImportSchema(RETAIL_IMPORT_SCHEMA_FILE)
        .asMain()
        .add().build();

    env.compile(bundle);

    env.execute();
  }

  @Test
  public void testImport() {
    String script = "IMPORT ecommerce-data.Orders;";
    run("c360", script);
  }

  class StubImportProcessor implements ImportProcessor {

    @Override
    public void process(ImportDefinition statement, Namespace namespace) {
      //Return a Logical Plan DAG of logical correlate queries

      

    }
  }
}
