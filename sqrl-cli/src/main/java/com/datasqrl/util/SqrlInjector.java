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

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ClasspathFunctionLoader;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.nio.file.Path;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.datasqrl")
public class SqrlInjector {

  @Bean
  public WorkspacePaths workspacePaths(
      @Qualifier("workspaceDir") Path workspaceDir,
      @Qualifier("buildDir") Path buildDir,
      @Qualifier("targetDir") Path targetDir,
      @Qualifier("projectRoot") Optional<Path> projectRoot) {

    return new WorkspacePaths(workspaceDir, buildDir, targetDir, projectRoot);
  }

  @Bean
  public ResourceResolver resourceResolver(WorkspacePaths workspacePaths) {
    return new FileResourceResolver(workspacePaths.buildDir());
  }

  @Bean
  public ClasspathFunctionLoader classpathFunctionLoader() {
    return new ClasspathFunctionLoader();
  }

  @Bean
  public NameCanonicalizer nameCanonicalizer() {
    return NameCanonicalizer.SYSTEM;
  }

  @Bean
  public JBangRunner jBangRunner(@Qualifier("internalTestExec") Boolean internalTestExec) {
    return internalTestExec ? JBangRunner.disabled() : JBangRunner.create();
  }

  @Bean
  public ModuleLoader moduleLoader(
      ResourceResolver resourceResolver,
      WorkspacePaths workspacePaths,
      ClasspathFunctionLoader classpathFunctionLoader,
      ErrorCollector errors) {

    return new ModuleLoaderImpl(resourceResolver, workspacePaths, classpathFunctionLoader, errors);
  }

  @Bean
  public ModuleLoader rootModuleLoader(
      PackageJson packageJson,
      WorkspacePaths workspacePaths,
      ClasspathFunctionLoader classpathFunctionLoader,
      ErrorCollector errors) {

    var sharedConfigs = packageJson.getScriptConfig().getSharedScriptConfigs();
    if (sharedConfigs.isEmpty()) {
      return null;
    }

    return new ModuleLoaderImpl(
        new FileResourceResolver(workspacePaths.buildDir()),
        workspacePaths,
        classpathFunctionLoader,
        errors);
  }

  @Bean
  public ExecutionEnginesHolder executionEnginesHolder(
      ErrorCollector errors,
      ApplicationContext applicationContext,
      PackageJson sqrlConfig,
      ExecutionGoal goal) {

    return new ExecutionEnginesHolder(
        errors, applicationContext, sqrlConfig, goal == ExecutionGoal.TEST);
  }
}
