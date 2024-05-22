///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.packager.config;
//
//import static com.datasqrl.discovery.DataDiscoveryFactory.getMetaDataStoreProvider;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//
//import com.datasqrl.config.PackageJson.CompilerConfig;
//import com.datasqrl.config.Dependency;
//import com.datasqrl.config.PackageJson.EnginesConfig;
//import com.datasqrl.config.PackageJson.ExplainConfig;
//import com.datasqrl.config.IPackageJson;
//import com.datasqrl.config.PackageConfiguration;
//import com.datasqrl.config.SqrlConfigCommons;
//import com.datasqrl.error.ErrorCollector;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Map;
//import lombok.SneakyThrows;
//import org.junit.jupiter.api.Test;
//
//public class ConfigurationTest {
//
//  public static final Path RESOURCE_DIR = Paths.get("src", "test", "resources");
//
//  @Test
//  @SneakyThrows
//  public void testConfiguration() {
//    //System.out.println(RESOURCE_DIR.toAbsolutePath());
//    ErrorCollector errors = ErrorCollector.root();
//    IPackageJson config = SqrlConfigCommons.fromFilesPackageJson(errors, RESOURCE_DIR.resolve("package-configtest.json"));
//    assertNotNull(config);
//    CompilerConfig compilerConfig = config.getCompilerConfig();
//
//    EnginesConfig engineConfig = config.getEngines();
//    assertEquals(1, config.getVersion());
//    assertEquals(1, engineConfig.getVersion());
//
//
//    assertFalse(compilerConfig.isAddArguments());
//    ExplainConfig explain = compilerConfig.getExplain();
//    assertFalse(explain.isText());
//    assertFalse(explain.isExtended());
//    assertTrue(explain.isVisual());
//    assertTrue(explain.isSorted());
//    assertEquals(2, engineConfig.size());
////    ExecutionPipeline executionPipeline = SimplePipeline.of();
////    pipelineFactory.getDatabaseEngine();
////    pipelineFactory.getStreamEngine();
////    getMetaDataStoreProvider(engineConfig.getDatabaseEngine());
//
//    Map<String, Dependency> dependencies = config.getDependencies().getDependencies();
//    assertEquals(2, dependencies.size());
//    assertEquals(dependencies.get("datasqrl.examples.Basic").getVersion(), "1.0.0");
//    assertEquals(dependencies.get("datasqrl.examples.Shared").getVariant(), "dev");
//
//    PackageConfiguration pkgConfig = config.getPackageConfig();
//    assertEquals("1.0.0", pkgConfig.getVersion());
//    assertEquals("dev", pkgConfig.getVariant());
//    assertEquals(3, pkgConfig.getTopics().size());
//
//    assertFalse(errors.hasErrorsWarningsOrNotices());
//  }
//
//}
