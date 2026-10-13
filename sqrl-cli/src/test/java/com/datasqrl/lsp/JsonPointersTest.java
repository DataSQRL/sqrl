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
package com.datasqrl.lsp;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.junit.jupiter.api.Test;

class JsonPointersTest {

  private static final String JSON =
      """
      {
        "enabled-engines": ["flink", "nope"],
        "script": {"main": "main.sqrl"}
      }
      """;

  @Test
  void givenPointerToValue_whenFind_thenRangeOfTheValue() {
    assertThat(JsonPointers.find(JSON, "/enabled-engines/1"))
        .contains(new Range(new Position(1, 31), new Position(1, 37)));
    assertThat(JsonPointers.find(JSON, "/script/main"))
        .contains(new Range(new Position(2, 21), new Position(2, 32)));
  }

  @Test
  void givenPointerToObject_whenFind_thenRangeOfItsName() {
    assertThat(JsonPointers.find(JSON, "/script"))
        .contains(new Range(new Position(2, 2), new Position(2, 10)));
    assertThat(JsonPointers.find(JSON, "/missing")).isEmpty();
  }

  @Test
  void givenMessageOfValidation_whenLocate_thenLastPackageFileWithTheValue() {
    var base = Path.of("package.json");
    var overlay = Path.of("test-package.json");
    var files = Map.of(base, JSON, overlay, "{\"script\": {\"main\": \"test.sqrl\"}}");

    var located =
        JsonPointers.locate(
            "does not match at location [/script/main]",
            List.of(base, overlay),
            file -> Optional.ofNullable(files.get(file)));

    assertThat(located)
        .contains(
            new JsonPointers.FileRange(
                overlay, new Range(new Position(0, 20), new Position(0, 31))));
    assertThat(JsonPointers.locate("no location", List.of(base), f -> Optional.of(JSON))).isEmpty();
  }
}
