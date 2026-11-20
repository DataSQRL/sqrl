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
package com.datasqrl.planner.contract;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorLocation.FileLocation;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class YamlLocationsTest {

  private static final String YAML =
      """
      apiVersion: v3.2.0
      schema:
        - name: Customers
          properties:
            - name: id
              required: true
            - name: email
      flow: {a: 1, b: [2, 3]}
      "a/b": slash
      "c~d": tilde
      """;

  @Test
  @SneakyThrows
  void givenNestedMapsAndSequences_whenLocate_thenReturnsLineAndColumnOfTheValue() {
    var locations = YamlLocations.of(YAML);

    assertThat(locations.locate("")).isEqualTo(new FileLocation(1, 1));
    assertThat(locations.locate("/apiVersion")).isEqualTo(new FileLocation(1, 13));
    assertThat(locations.locate("/schema/0/name")).isEqualTo(new FileLocation(3, 11));
    assertThat(locations.locate("/schema/0/properties/0/required"))
        .isEqualTo(new FileLocation(6, 19));
    assertThat(locations.locate("/schema/0/properties/1").line()).isEqualTo(7);
  }

  @Test
  @SneakyThrows
  void givenFlowStyle_whenLocate_thenReturnsLocationInTheLine() {
    var locations = YamlLocations.of(YAML);

    assertThat(locations.locate("/flow/a")).isEqualTo(new FileLocation(8, 11));
    assertThat(locations.locate("/flow/b/1")).isEqualTo(new FileLocation(8, 21));
  }

  @Test
  @SneakyThrows
  void givenKeysWithSlashAndTilde_whenLocate_thenUsesEscapedPointers() {
    var locations = YamlLocations.of(YAML);

    assertThat(locations.locate("/a~1b").line()).isEqualTo(9);
    assertThat(locations.locate("/c~0d").line()).isEqualTo(10);
  }

  @Test
  @SneakyThrows
  void givenPointerOfMissingValue_whenLocate_thenFallsBackToNearestParent() {
    var locations = YamlLocations.of(YAML);

    assertThat(locations.locate("/schema/0/properties/0/logicalType"))
        .isEqualTo(locations.locate("/schema/0/properties/0"));
    assertThat(locations.locate("/missing/deeply/nested")).isEqualTo(FileLocation.START);
  }

  @Test
  @SneakyThrows
  void givenSeveralDocuments_whenOf_thenOnlyTheFirstIsIndexed() {
    var locations = YamlLocations.of("a: 1\n---\nb: 2\n");

    assertThat(locations.locate("/a")).isEqualTo(new FileLocation(1, 4));
    assertThat(locations.locate("/b")).isEqualTo(FileLocation.START);
  }
}
