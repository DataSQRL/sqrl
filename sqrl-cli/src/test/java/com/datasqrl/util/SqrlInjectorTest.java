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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.MergedAnnotations;

class SqrlInjectorTest {

  @Test
  void given_allComponentsOnClasspath_when_scanned_then_everyComponentIsImported() {
    var scanned =
        new ClassPathScanningCandidateComponentProvider(true)
            .findCandidateComponents("com.datasqrl").stream()
                .map(definition -> definition.getBeanClassName())
                .filter(name -> !isConfiguration(name))
                .collect(Collectors.toSet());

    assertThat(importedComponents(SqrlInjector.class)).isEqualTo(scanned);
  }

  private static Set<String> importedComponents(Class<?> configuration) {
    return Stream.of(MergedAnnotations.from(configuration).get(Import.class).getClassArray("value"))
        .flatMap(
            imported ->
                isConfiguration(imported)
                    ? importedComponents(imported).stream()
                    : Stream.of(imported.getName()))
        .collect(Collectors.toSet());
  }

  private static boolean isConfiguration(String className) {
    try {
      return isConfiguration(Class.forName(className));
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean isConfiguration(Class<?> clazz) {
    return MergedAnnotations.from(clazz).isPresent(Configuration.class);
  }
}
