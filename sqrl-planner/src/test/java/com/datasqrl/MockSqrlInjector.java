package com.datasqrl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlFrameworkImpl;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.config.SqrlRelBuilder;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.SqrlPlanningTableFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;

public class MockSqrlInjector extends AbstractModule {

  private final ErrorCollector errors;
  private final SqrlConfig config;
  private final boolean debug;
  private final Path rootDir;
  private final Map<NamePath, SqrlModule> addlModules;
  private final Optional<Path> errorDir;

  public MockSqrlInjector(ErrorCollector errors, SqrlConfig config, Optional<Path> errorDir,
      Path rootDir, Map<NamePath, SqrlModule> addlModules, boolean isDebug) {
    this.errors = errors;
    this.config = config;
    this.rootDir = rootDir;
    this.debug = isDebug;
    this.errorDir = errorDir;
    this.addlModules = addlModules;
  }

  @Override
  public void configure() {
    bind(SqrlFramework.class).to(SqrlFrameworkImpl.class);
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(APIConnectorManager.class).to(APIConnectorManagerImpl.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(CompilerConfiguration.class).to(SqrlCompilerConfiguration.class);
    bind(SqrlTableFactory.class).to(SqrlPlanningTableFactory.class);
    bind(RelBuilder.class).to(SqrlRelBuilder.class);
    bind(ModuleLoader.class).to(ModuleLoaderImpl.class);
  }

  @Provides
  @Named("debugFlag")
  public boolean provideDebugFlag() {
    return debug;
  }

  @Provides
  public ErrorCollector provideErrorCollector() {
    return errors;
  }

  @Provides
  public NameCanonicalizer provideNameCanonicalizer() {
    return NameCanonicalizer.SYSTEM;
  }

  @Provides
  @Named("rootDir")
  public Path provideRootDir() {
    return rootDir;
  }

  @Provides
  @Named("buildDir")
  public Path provideBuildDir() {
    return rootDir.resolve("build");
  }

  @Provides
  @Named("targetDir")
  public Path provideTargetDir() {
    return rootDir.resolve("build").resolve("deploy");
  }

  @Provides
  @Named("errorDir")
  public Optional<Path> provideErrorDir() {
    return errorDir;
  }

  @Provides
  @Named("addlModules")
  public Map<NamePath, SqrlModule> provideAddlModules() {
    return addlModules;
  }

  @Provides
  public ResourceResolver provideResourceResolver() {
    if (rootDir == null) {
      return new FileResourceResolver(Path.of("../sqrl-testing/sqrl-integration-tests/src/test/resources/dagplanner"));
    }
    return new FileResourceResolver(rootDir);
  }


  @Provides
  public SqrlConfig provideSqrlConfig() {
    return config;
  }

}
