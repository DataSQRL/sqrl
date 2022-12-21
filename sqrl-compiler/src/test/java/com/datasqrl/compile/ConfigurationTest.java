/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compile;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalCompilerConfiguration;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.error.ErrorCollector;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationTest {

  public static final Path RESOURCE_DIR = Paths.get("src", "test", "resources");

  @Test
  @SneakyThrows
  public void testConfiguration() {
    GlobalCompilerConfiguration config = GlobalEngineConfiguration.readFrom(
        RESOURCE_DIR.resolve("package1.json"), GlobalCompilerConfiguration.class);
    assertNotNull(config);
    assertEquals(3, config.getCompiler().getApi().getMaxArguments());
    assertEquals(2, config.getEngines().size());
    ErrorCollector errors = ErrorCollector.root();
    EngineSettings engineSettings = config.initializeEngines(errors);
    assertNotNull(engineSettings, errors.toString());
    assertNotNull(engineSettings.getPipeline());
  }

}
