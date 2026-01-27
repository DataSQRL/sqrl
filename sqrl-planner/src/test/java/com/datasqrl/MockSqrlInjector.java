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
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import java.nio.file.Path;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MockSqrlInjector {

  @Bean
  public RelDataTypeFactory relDataTypeFactory() {
    return new TypeFactory();
  }

  @Bean
  public MainScript mainScript(PackageJson packageJson, ResourceResolver resourceResolver) {
    return new MainScriptImpl(packageJson, resourceResolver);
  }

  @Bean
  public ExecutionPipeline executionPipeline(ApplicationContext applicationContext) {
    return new SqrlConfigPipeline(applicationContext);
  }

  @Bean
  public CompilerConfig compilerConfig(PackageJson packageJson) {
    return new SqrlCompilerConfiguration(packageJson);
  }

  @Bean
  public ModuleLoader moduleLoader(
      ResourceResolver resourceResolver, BuildPath buildPath, ErrorCollector errors) {
    return new ModuleLoaderImpl(resourceResolver, buildPath, errors);
  }

  @Bean
  public NameCanonicalizer nameCanonicalizer() {
    return NameCanonicalizer.SYSTEM;
  }

  @Bean
  @Qualifier("buildDir")
  public Path buildDir(@Qualifier("rootDir") Path rootDir) {
    return rootDir.resolve("build");
  }

  @Bean
  @Qualifier("targetDir")
  public Path targetDir(@Qualifier("rootDir") Path rootDir) {
    return rootDir.resolve("build").resolve("deploy");
  }

  @Bean
  public BuildPath buildPath(
      @Qualifier("buildDir") Path buildDir, @Qualifier("targetDir") Path targetDir) {
    return new BuildPath(buildDir, targetDir);
  }

  @Bean
  public ResourceResolver resourceResolver(@Qualifier("rootDir") Path rootDir) {
    if (rootDir == null) {
      return new FileResourceResolver(
          Path.of("../sqrl-testing/sqrl-testing-integration/src/test/resources/dagplanner"));
    }
    return new FileResourceResolver(rootDir);
  }
}
