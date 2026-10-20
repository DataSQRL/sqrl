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
package com.datasqrl.loaders.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SchemaLoaderImplTest {

  @TempDir Path tempDir;

  @ParameterizedTest
  @ValueSource(strings = {"data.csv", "data.csv.gz"})
  void givenCsvFile_whenLoadSchema_thenFailsAndAsksToDeclareColumns(String file) {
    var schemaLoader =
        new SchemaLoaderImpl(new FileResourceResolver(tempDir), ErrorCollector.root());

    assertThatThrownBy(() -> schemaLoader.loadSchema("Readings", file, Map.of()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Cannot infer the schema of CSV file [%s]", file)
        .hasMessageContaining("Declare the columns of table [Readings]");
  }

  @Test
  void givenNameWithoutSchemaExtension_whenLoadSchema_thenReturnsEmpty() {
    var schemaLoader =
        new SchemaLoaderImpl(new FileResourceResolver(tempDir), ErrorCollector.root());

    assertThat(schemaLoader.loadSchema("Readings", "OtherTable", Map.of())).isEmpty();
  }
}
