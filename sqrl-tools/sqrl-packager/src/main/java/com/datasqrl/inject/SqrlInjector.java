package com.datasqrl.inject;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlFrameworkImpl;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.ConnectorFactoryFactoryImpl;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TableConfigLoader;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.config.SqrlRelBuilder;
import com.datasqrl.config.TableConfigLoaderImpl;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.SqrlPlanningTableFactory;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import java.nio.file.Path;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;

public class SqrlInjector extends AbstractModule {

  private final ErrorCollector errors;
  private final Path rootDir;
  private final Path buildDir;
  private final Path targetDir;
  private final boolean debug;
  private final PackageJson sqrlConfig;
  private final ExecutionGoal goal;

  public SqrlInjector(ErrorCollector errors, Path rootDir, Path targetDir, boolean debug,
      PackageJson sqrlConfig, ExecutionGoal goal) {
    this.errors = errors;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve("build");
    this.targetDir = targetDir;
    this.debug = debug;
    this.sqrlConfig = sqrlConfig;
    this.goal = goal;
  }

  @Override
  public void configure() {
    bind(SqrlFramework.class).to(SqrlFrameworkImpl.class);
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(APIConnectorManager.class).to(APIConnectorManagerImpl.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(ModuleLoader.class).to(ModuleLoaderImpl.class);
    bind(CompilerConfig.class).to(SqrlCompilerConfiguration.class);
    bind(SqrlTableFactory.class).to(SqrlPlanningTableFactory.class);
    bind(RelBuilder.class).to(SqrlRelBuilder.class);
    bind(TableConfigLoader.class).to(TableConfigLoaderImpl.class);
    bind(ConnectorFactoryFactory.class).to(ConnectorFactoryFactoryImpl.class);
  }

  @Provides
  @Named("buildDir")
  public Path provideBuildDir() {
    return buildDir;
  }

  @Provides
  @Named("targetDir")
  public Path provideTargetDir() {
    return targetDir;
  }

  @Provides
  @Named("debugFlag")
  public boolean provideDebugFlag() {
    return debug;
  }

  @Provides
  public ResourceResolver provideResourceResolver() {
    return new FileResourceResolver(buildDir);
  }

  @Provides
  public NameCanonicalizer provideNameCanonicalizer() {
    return NameCanonicalizer.SYSTEM;
  }

  @Provides
  public PackageJson provideSqrlConfig() {
    return sqrlConfig;
  }

  @Provides
  public ExecutionGoal provideExecutionGoal() {
    return goal;
  }

  @Provides
  public ErrorCollector provideErrorCollector() {
    return errors;
  }

}
