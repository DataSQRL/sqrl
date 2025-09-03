/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.ConnectorFactoryFactoryImpl;
import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.QueryEngineConfigConverter;
import com.datasqrl.config.QueryEngineConfigConverterImpl;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.JBangPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import java.nio.file.Path;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class SqrlInjector extends AbstractModule {

  private final ErrorCollector errors;
  private final Path rootDir;
  private final Path buildDir;
  private final Path targetDir;
  private final PackageJson sqrlConfig;
  private final ExecutionGoal goal;
  private final boolean internalTestExec;

  public SqrlInjector(
      ErrorCollector errors,
      Path rootDir,
      Path targetDir,
      PackageJson sqrlConfig,
      ExecutionGoal goal,
      boolean internalTestExec) {
    this.errors = errors;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
    this.targetDir = targetDir;
    this.sqrlConfig = sqrlConfig;
    this.goal = goal;
    this.internalTestExec = internalTestExec;
  }

  @Override
  public void configure() {
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(ModuleLoader.class).to(ModuleLoaderImpl.class);
    bind(CompilerConfig.class).to(SqrlCompilerConfiguration.class);
    bind(ConnectorFactoryFactory.class).to(ConnectorFactoryFactoryImpl.class);
    bind(QueryEngineConfigConverter.class).to(QueryEngineConfigConverterImpl.class);

    Multibinder<Preprocessor> binder = Multibinder.newSetBinder(binder(), Preprocessor.class);
    binder.addBinding().to(CopyStaticDataPreprocessor.class);
    binder.addBinding().to(JBangPreprocessor.class);
    binder.addBinding().to(JarPreprocessor.class);
  }

  @Provides
  @Named("buildDir")
  public Path provideBuildDir() {
    return buildDir;
  }

  @Provides
  @Named("rootDir")
  public Path provideRootDir() {
    return rootDir;
  }

  @Provides
  @Named("targetDir")
  public Path provideTargetDir() {
    return targetDir;
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
  @Singleton
  public JBangRunner provideJBangRunner() {
    return internalTestExec ? JBangRunner.disabled() : JBangRunner.create();
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

  @Provides
  public ExecutionEnginesHolder provideExecutionEnginesHolder(Injector injector) {
    return new ExecutionEnginesHolder(errors, injector, sqrlConfig, goal == ExecutionGoal.TEST);
  }
}
