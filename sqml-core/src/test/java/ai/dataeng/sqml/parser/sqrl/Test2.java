package ai.dataeng.sqml.parser.sqrl;

import static ai.dataeng.sqml.parser.operator.C360Test.*;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.parser.Script;
import ai.dataeng.sqml.parser.operator.C360Test;
import ai.dataeng.sqml.parser.operator.DefaultTestSettings;
import com.google.common.collect.ImmutableList;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Test2 {

  private VertxInternal vertx;
  private Environment env;

  @BeforeEach
  public void setup() throws IOException {
    VertxOptions vertxOptions = new VertxOptions();
    this.vertx = (VertxInternal) Vertx.vertx(vertxOptions);

//    FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    SqrlSettings settings = ConfigurationTest.getDefaultSettings(false);

    env = Environment.create(settings);

    env.getDatasetRegistry().addOrUpdateSource(dd, ErrorCollector.root());
//    registerDatasets();
  }


  private void registerDatasets() {
//    ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();

    String ds2Name = "ecommerce-data";
    FileSourceConfiguration fileConfig = FileSourceConfiguration.builder()
        .uri(C360Test.RETAIL_DATA_DIR.toAbsolutePath().toString())
        .name(ds2Name)
        .build();
    env.getDatasetRegistry().addOrUpdateSource(fileConfig, ErrorCollector.root());
//    assertFalse(errors.isFatal());


    //Needs some time to wait for the flink pipeline to compile data
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  @Test
  public void test() {
    //c360, test import all the way through to query
    Script script = run(
        "IMPORT ecommerce-data.Customer;\n"
            + "IMPORT ecommerce-data.Product;\n"
            + "IMPORT ecommerce-data.Orders;\n"
            + "\n"
            + "Orders.entries.discount := discount + 1;\n"
    );

    System.out.println(script.getGraphQL().execute("query { orders { data { entries { discount } } } }"));

  }

  private Script run(String script) {
    try {
      ScriptBundle bundle = ScriptBundle.Config.builder()
          .name(RETAIL_SCRIPT_NAME)
          .scripts(ImmutableList.of(
              SqrlScript.Config.builder()
                  .name(RETAIL_SCRIPT_NAME)
                  .main(true)
                  .content(script)
                  .inputSchema(Files.readString(RETAIL_IMPORT_SCHEMA_FILE))
                  .build()
          ))
          .build().initialize(ErrorCollector.root());
      return env.compile(ScriptDeployment.of(bundle));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
