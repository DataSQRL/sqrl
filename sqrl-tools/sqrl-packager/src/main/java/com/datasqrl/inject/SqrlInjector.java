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
package com.datasqrl.inject;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.ConnectorFactoryFactoryImpl;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.discovery.preprocessor.FlexibleSchemaInferencePreprocessor;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.avro.AvroSchemaPreprocessor;
import com.datasqrl.io.schema.flexible.FlexibleSchemaPreprocessor;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.DataSystemPreprocessor;
import com.datasqrl.packager.preprocess.FlinkSqlPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.ScriptPreprocessor;
import com.datasqrl.packager.preprocess.TablePreprocessor;
import com.datasqrl.packager.preprocessor.Preprocessor;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
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

  public SqrlInjector(
      ErrorCollector errors,
      Path rootDir,
      Path targetDir,
      PackageJson sqrlConfig,
      ExecutionGoal goal) {
    this.errors = errors;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
    this.targetDir = targetDir;
    this.sqrlConfig = sqrlConfig;
    this.goal = goal;
  }

  @Override
  public void configure() {
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(ModuleLoader.class).to(ModuleLoaderImpl.class);
    bind(CompilerConfig.class).to(SqrlCompilerConfiguration.class);
    bind(ConnectorFactoryFactory.class).to(ConnectorFactoryFactoryImpl.class);

    Multibinder<Preprocessor> binder = Multibinder.newSetBinder(binder(), Preprocessor.class);
    binder.addBinding().to(ScriptPreprocessor.class);
    binder.addBinding().to(TablePreprocessor.class);
    binder.addBinding().to(CopyStaticDataPreprocessor.class);
    binder.addBinding().to(JarPreprocessor.class);
    binder.addBinding().to(DataSystemPreprocessor.class);
    binder.addBinding().to(FlinkSqlPreprocessor.class);
    binder.addBinding().to(FlexibleSchemaPreprocessor.class);
    binder.addBinding().to(AvroSchemaPreprocessor.class);
    binder.addBinding().to(FlexibleSchemaInferencePreprocessor.class);
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
