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
  public ResourceResolver resourceResolver(@Qualifier("buildDir") Path buildDir) {
    return new FileResourceResolver(buildDir);
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
  public ExecutionEnginesHolder executionEnginesHolder(
      ErrorCollector errors,
      ApplicationContext applicationContext,
      PackageJson sqrlConfig,
      ExecutionGoal goal) {

    return new ExecutionEnginesHolder(
        errors, applicationContext, sqrlConfig, goal == ExecutionGoal.TEST);
  }
}
