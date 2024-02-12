package com.datasqrl.inject;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlFrameworkImpl;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigDebugger;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.config.SqrlConfigTableSink;
import com.datasqrl.config.SqrlRelBuilder;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ObjectLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.SqrlPlanningTableFactory;
import com.datasqrl.plan.local.generate.Debugger;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import java.nio.file.Path;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;

public class MockSqrlInjector extends AbstractModule {

  private final ModuleLoader moduleLoader;
  private final ErrorCollector errors;
  private final SqrlConfig config;
  private final boolean debug;

  public MockSqrlInjector(ModuleLoader moduleLoader,
      ErrorCollector errors, SqrlConfig config) {
    this.moduleLoader = moduleLoader;
    this.errors = errors;
    this.config = config;

//    this.rootDir = rootDir;
//    this.buildDir = rootDir.resolve("build");
//    this.targetDir = targetDir;
    this.debug = false;
//    this.sqrlConfig = sqrlConfig;
  }

  @Override
  public void configure() {
    bind(SqrlFramework.class).to(SqrlFrameworkImpl.class);
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(APIConnectorManager.class).to(APIConnectorManagerImpl.class);
    bind(ObjectLoader.class).to(ObjectLoaderImpl.class);
    bind(Debugger.class).to(SqrlConfigDebugger.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(TableSink.class).to(SqrlConfigTableSink.class);
    bind(CompilerConfiguration.class).to(SqrlCompilerConfiguration.class);
    bind(SqrlTableFactory.class).to(SqrlPlanningTableFactory.class);
    bind(RelBuilder.class).to(SqrlRelBuilder.class);
  }

  @Provides
  @Named("debugFlag")
  public boolean provideDebugFlag() {
    return debug;
  }

  @Provides
  public ModuleLoader provideModuleLoader() {
    return moduleLoader;
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
  @Named("buildDir")
  public Path provideBuildDir() {
    return null;
  }

  @Provides
  @Named("targetDir")
  public Path provideTargetDir() {
    return null;
  }

  @Provides
  public ResourceResolver provideResourceResolver() {
    return null;
  }


  @Provides
  public SqrlConfig provideSqrlConfig() {
    return config;
  }

}
