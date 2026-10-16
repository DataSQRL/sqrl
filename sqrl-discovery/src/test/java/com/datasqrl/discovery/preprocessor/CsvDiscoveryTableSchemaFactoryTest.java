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
package com.datasqrl.discovery.preprocessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvDiscoveryTableSchemaFactoryTest {

  @TempDir Path tempDir;

  private final CsvDiscoveryTableSchemaFactory underTest = new CsvDiscoveryTableSchemaFactory();

  @Test
  void givenCsvWithHeaderRow_whenConvert_thenGeneratesFlexibleCsvSourceThatSkipsHeader()
      throws IOException {
    var file = Files.writeString(tempDir.resolve("person.csv"), "id, name\n1, Robert\n");

    var result = underTest.convert(file, Map.of(), ErrorCollector.root());

    assertThat(result.type().getFieldNames()).containsExactly("id", "name");
    assertThat(result.connectorOptions())
        .containsEntry("format", "flexible-csv")
        .containsEntry("flexible-csv.skip-header", "true");
  }

  @Test
  void givenCsvWithoutHeaderRow_whenConvert_thenFailsWithHeaderRowError() throws IOException {
    var file = Files.writeString(tempDir.resolve("person.csv"), "1, Robert\n2, Abduhla\n");

    assertThatThrownBy(() -> underTest.convert(file, Map.of(), ErrorCollector.root()))
        .hasMessage(
            "Could not infer schema for data file [%s]: The first line must be a header row of"
                + " column names that each start with a letter, but was: 1, Robert",
            file);
  }

  @Test
  void givenEmptyCsv_whenConvert_thenFailsWithNoRecordsError() throws IOException {
    var file = Files.writeString(tempDir.resolve("person.csv"), "");

    assertThatThrownBy(() -> underTest.convert(file, Map.of(), ErrorCollector.root()))
        .hasMessage(
            "Could not infer schema for data file [%s] because it contained no valid records",
            file);
  }
}
