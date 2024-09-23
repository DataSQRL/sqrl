package com.datasqrl.config;

import static com.datasqrl.config.ConfigurationTest.CONFIG_DIR;
import static com.datasqrl.config.ConfigurationTest.testForErrors;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ConfigurationSchemaTest {


  @Test
  public void readTableConfigsNormal() {
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfigCommons.fromFilesTableConfig(Name.system("MyTable"), errors, List.of(CONFIG_DIR.resolve("tableConfigFull.json")));
  }

  @Test
  public void readTableConfigsMissing() {
    testForErrors(errors -> SqrlConfigCommons.fromFilesTableConfig(Name.system("MyTable"), errors,
        List.of(CONFIG_DIR.resolve("tableConfigMissing.json"))));
  }

  @Test
  public void readTableExtraKeys() {
    testForErrors(errors -> SqrlConfigCommons.fromFilesTableConfig(Name.system("MyTable"), errors,
        List.of(CONFIG_DIR.resolve("tableConfigExtraField.json"))));
  }

}
