/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.config;

import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import java.util.LinkedHashMap;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigurationTest {

  public static final Path RESOURCE_DIR = Paths.get("src", "test", "resources");

  @Test
  @SneakyThrows
  public void testConfiguration() {
    //System.out.println(RESOURCE_DIR.toAbsolutePath());
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, RESOURCE_DIR.resolve("package-configtest.json"));
    assertNotNull(config);
    CompilerConfiguration compilerConfig = CompilerConfiguration.fromRootConfig(config);
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);


    assertEquals(3, compilerConfig.getMaxApiArguments());
    assertEquals(2, pipelineFactory.getEngines().size());
    ExecutionPipeline executionPipeline = pipelineFactory.createPipeline();
    pipelineFactory.getDatabaseEngine();
    pipelineFactory.getStreamEngine();
    pipelineFactory.getMetaDataStoreProvider(Optional.empty());

    LinkedHashMap<String, Dependency> dependencies = DependencyConfig.fromRootConfig(config);
    assertEquals(2, dependencies.size());
    assertEquals(dependencies.get("datasqrl.examples.Basic").getVersion(), "1.0.0");
    assertEquals(dependencies.get("datasqrl.examples.Shared").getVariant(), "dev");

    PackageConfiguration pkgConfig = PackageConfiguration.fromRootConfig(config);
    assertEquals("1.0.0", pkgConfig.getVersion());
    assertEquals("dev", pkgConfig.getVariant());

    assertFalse(errors.hasErrors());
  }

}
