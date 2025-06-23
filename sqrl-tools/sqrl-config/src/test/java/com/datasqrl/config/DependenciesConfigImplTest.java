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
package com.datasqrl.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DependenciesConfigImplTest {

  private SqrlConfig rootConfig;
  private SqrlConfig dependenciesConfig;

  @BeforeEach
  void setUp() {
    rootConfig = SqrlConfig.createCurrentVersion();
    dependenciesConfig = rootConfig.getSubConfig("dependencies");
  }

  @Test
  void givenConfigWithDependencies_whenGetDependency_thenReturnsDependency() {
    dependenciesConfig.getSubConfig("dep1").setProperty("name", "dependency1");
    dependenciesConfig.getSubConfig("dep2").setProperty("name", "dependency2");

    DependenciesConfigImpl dependenciesConfigImpl =
        new DependenciesConfigImpl(rootConfig, dependenciesConfig);

    assertThat(dependenciesConfigImpl.getDependency("dep1")).isPresent();
    assertThat(dependenciesConfigImpl.getDependency("dep2")).isPresent();
    assertThat(dependenciesConfigImpl.getDependency("nonexistent")).isEmpty();
  }

  @Test
  void givenDependenciesConfig_whenAddDependency_thenAddsDependencyToConfig() {
    DependenciesConfigImpl dependenciesConfigImpl =
        new DependenciesConfigImpl(rootConfig, dependenciesConfig);
    DependencyImpl dependency = new DependencyImpl("test-dependency");

    dependenciesConfigImpl.addDependency("test", dependency);

    assertThat(dependenciesConfigImpl.getDependency("test")).isPresent();
  }

  @Test
  void givenConfigWithDependencies_whenGetDependencies_thenReturnsAllDependencies() {
    dependenciesConfig.getSubConfig("dep1").setProperty("name", "dependency1");
    dependenciesConfig.getSubConfig("dep2").setProperty("name", "dependency2");

    DependenciesConfigImpl dependenciesConfigImpl =
        new DependenciesConfigImpl(rootConfig, dependenciesConfig);

    Map<String, DependencyImpl> dependencies = dependenciesConfigImpl.getDependencies();

    assertThat(dependencies).hasSize(2);
    assertThat(dependencies).containsKey("dep1");
    assertThat(dependencies).containsKey("dep2");
  }
}
