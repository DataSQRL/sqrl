package ai.dataeng.sqml.parser.sqrl;

import static ai.dataeng.sqml.parser.operator.C360Test.*;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.parser.Script;
import ai.dataeng.sqml.parser.operator.DefaultTestSettings;
import com.google.common.collect.ImmutableList;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Test2 {

  private VertxInternal vertx;
  private Environment env;

  @BeforeEach
  public void setup() {
    VertxOptions vertxOptions = new VertxOptions();
    this.vertx = (VertxInternal) Vertx.vertx(vertxOptions);

    env = Environment.create(DefaultTestSettings.create(vertx));

    env.getDatasetRegistry().addOrUpdateSource(dd, ErrorCollector.root());

  }

  @Test
  public void test() {
    //c360, test import all the way through to query
    Script script = run("IMPORT ecommerce-data.Customer;");

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
