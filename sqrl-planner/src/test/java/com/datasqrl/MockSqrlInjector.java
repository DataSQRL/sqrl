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

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(
    basePackages = {
      "com.datasqrl.calcite.type",
      "com.datasqrl.config",
      "com.datasqrl.server",
      "com.datasqrl.loaders",
      "com.datasqrl.planner"
    })
public class MockSqrlInjector {

  @Bean
  public NameCanonicalizer nameCanonicalizer() {
    return NameCanonicalizer.SYSTEM;
  }

  @Bean
  public WorkspacePaths workspacePaths(@Qualifier("workspaceDir") Path workspaceDir) {
    return new WorkspacePaths(
        workspaceDir,
        workspaceDir.resolve("build"),
        workspaceDir.resolve("build").resolve("deploy"),
        Optional.of(workspaceDir));
  }

  @Bean
  public ResourceResolver resourceResolver(@Qualifier("workspaceDir") Path workspaceDir) {
    return new FileResourceResolver(
        Objects.requireNonNullElseGet(
            workspaceDir,
            () ->
                Path.of("../sqrl-testing/sqrl-testing-integration/src/test/resources/dagplanner")));
  }
}
