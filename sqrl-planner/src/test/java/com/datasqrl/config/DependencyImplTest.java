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

import com.datasqrl.error.ErrorCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DependencyImplTest {

  private ErrorCollector errors;
  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    errors = ErrorCollector.root();
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenConfigWithFolder_whenCreateFromConfig_thenSetsName() {
    config.setProperty("folder", "test-dependency");

    var dependency = new DependencyImpl(config);

    assertThat(dependency.getFolder(null)).isEqualTo("test-dependency");
  }

  @Test
  void givenConfigWithName_whenCreateFromConfig_thenSetsName() {
    config.setProperty("name", "test-dependency");

    var dependency = new DependencyImpl(config);

    assertThat(dependency.getFolder(null)).isEqualTo("test-dependency");
  }

  @Test
  void givenEmptyConfig_whenCreateFromConfig_thenNameIsNull() {
    var dependency = new DependencyImpl(config);

    assertThat(dependency.getFolder(null)).isNull();
  }

  @Test
  void givenNoArgs_whenCreateDependency_thenNameIsNull() {
    var dependency = new DependencyImpl();

    assertThat(dependency.getFolder(null)).isNull();
  }

  @Test
  void givenName_whenCreateDependency_thenSetsName() {
    var dependency = new DependencyImpl("named-dep");

    assertThat(dependency.getFolder(null)).isEqualTo("named-dep");
  }

  @Test
  void givenDependencyWithoutName_whenNormalize_thenUsesDefaultName() {
    var dependency = new DependencyImpl();

    var normalized = dependency.normalize("default-name", errors);

    assertThat(normalized.getFolder(null)).isEqualTo("default-name");
  }

  @Test
  void givenDependencyWithName_whenNormalize_thenKeepsOriginalName() {
    var dependency = new DependencyImpl("explicit-name");

    var normalized = dependency.normalize("default-name", errors);

    assertThat(normalized.getFolder(null)).isEqualTo("explicit-name");
  }

  @Test
  void givenDependency_whenToString_thenReturnsName() {
    var dependency = new DependencyImpl("test-name");

    var result = dependency.toString();

    assertThat(result).isEqualTo("test-name");
  }
}
