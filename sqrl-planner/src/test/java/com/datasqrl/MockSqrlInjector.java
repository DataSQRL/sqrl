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
package com.datasqrl;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public class MockSqrlInjector extends AbstractModule {

  private final ErrorCollector errors;
  private final PackageJson config;
  private final Path rootDir;
  private final Map<NamePath, SqrlModule> addlModules;
  private final Optional<Path> errorDir;

  public MockSqrlInjector(
      ErrorCollector errors,
      PackageJson config,
      Optional<Path> errorDir,
      Path rootDir,
      Map<NamePath, SqrlModule> addlModules) {
    this.errors = errors;
    this.config = config;
    this.rootDir = rootDir;
    this.errorDir = errorDir;
    this.addlModules = addlModules;
  }

  @Override
  public void configure() {
    bind(RelDataTypeFactory.class).to(TypeFactory.class);
    bind(MainScript.class).to(MainScriptImpl.class);
    bind(ExecutionPipeline.class).to(SqrlConfigPipeline.class);
    bind(CompilerConfig.class).to(SqrlCompilerConfiguration.class);
    bind(ModuleLoader.class).to(ModuleLoaderImpl.class);
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
      return new FileResourceResolver(
          Path.of("../sqrl-testing/sqrl-testing-integration/src/test/resources/dagplanner"));
    }
    return new FileResourceResolver(rootDir);
  }

  @Provides
  public PackageJson provideSqrlConfig() {
    return config;
  }
}
