package com.datasqrl.config;

import static com.datasqrl.config.ConfigurationTest.CONFIG_DIR;
import static com.datasqrl.config.ConfigurationTest.testForErrors;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ConfigurationSchemaTest {

  private static final Path TEST_CASES = Path.of(CONFIG_DIR.toString(), "tableConfig");

  @Test
  public void readTableConfigsNormal() {
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfigCommons.fromFilesTableConfig(
        Name.system("MyTable"), errors, List.of(TEST_CASES.resolve("tableConfigFull.json")));
  }

  @Test
  public void readTableConfigsMissing() {
    testForErrors(
        errors ->
            SqrlConfigCommons.fromFilesTableConfig(
                Name.system("MyTable"),
                errors,
                List.of(TEST_CASES.resolve("tableConfigMissing.json"))));
  }

  @Test
  public void readTableExtraKeys() {
    testForErrors(
        errors ->
            SqrlConfigCommons.fromFilesTableConfig(
                Name.system("MyTable"),
                errors,
                List.of(TEST_CASES.resolve("tableConfigExtraField.json"))));
  }
}
