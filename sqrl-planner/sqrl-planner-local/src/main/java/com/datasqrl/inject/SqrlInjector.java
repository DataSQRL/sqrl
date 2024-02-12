package com.datasqrl.inject;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigDebugger;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlFrameworkImpl;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigTableSink;
import com.datasqrl.config.SqrlRelBuilder;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.ObjectLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.SqrlPlanningTableFactory;
import com.datasqrl.plan.local.generate.Debugger;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import java.nio.file.Path;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;

public class SqrlInjector extends AbstractModule {

  private final ErrorCollector errors;
  private final Path rootDir;
  private final Path buildDir;
  private final Path targetDir;
  private final boolean debug;
  private final SqrlConfig sqrlConfig;

  public SqrlInjector(ErrorCollector errors, Path rootDir, Path targetDir, boolean debug,
      SqrlConfig sqrlConfig) {
    this.errors = errors;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve("build");
    this.targetDir = targetDir;
    this.debug = debug;
    this.sqrlConfig = sqrlConfig;
  }

  @Override
  public void configure() {
    bind(SqrlFramework.class).to(SqrlFrameworkImpl.class);
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(APIConnectorManager.class).to(APIConnectorManagerImpl.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(ObjectLoader.class).to(ObjectLoaderImpl.class);
    bind(ModuleLoader.class).to(ModuleLoaderImpl.class);
    bind(Debugger.class).to(SqrlConfigDebugger.class);
    bind(TableSink.class).to(SqrlConfigTableSink.class);
    bind(CompilerConfiguration.class).to(SqrlCompilerConfiguration.class);
    bind(SqrlTableFactory.class).to(SqrlPlanningTableFactory.class);
    bind(RelBuilder.class).to(SqrlRelBuilder.class);
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
  public SqrlConfig provideSqrlConfig() {
    return sqrlConfig;
  }

  @Provides
  public ErrorCollector provideErrorCollector() {
    return errors;
  }

}
