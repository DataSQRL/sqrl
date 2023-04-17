package com.datasqrl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {

  public static final Path CONFIG_DIR = Paths.get("src","test","resources","config");
  public static final Path CONFIG_FILE1 = CONFIG_DIR.resolve("config1.json");

  @Test
  public void testJsonConfiguration() {
    System.out.println(CONFIG_DIR.toAbsolutePath().toString());
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1);
    assertEquals(5, config.keyInt("key2").get());
  }

}
