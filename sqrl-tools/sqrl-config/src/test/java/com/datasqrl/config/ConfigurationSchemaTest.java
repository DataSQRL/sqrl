package com.datasqrl.config;

import static com.datasqrl.config.ConfigurationTest.CONFIG_DIR;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
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

  public void testForErrors(Consumer<ErrorCollector> failure) {
    ErrorCollector errors = ErrorCollector.root();
    try {
      failure.accept(errors);
      fail();
    } catch (Exception e) {
      System.out.println(ErrorPrinter.prettyPrint(errors));
      assertTrue(errors.isFatal());
    }
  }

}
