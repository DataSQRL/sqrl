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
package com.datasqrl.discovery.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CSVRecordReaderTest {

  private final CSVRecordReader underTest = new CSVRecordReader();

  @Test
  void givenHeaderRow_whenRead_thenRecordsAreKeyedByColumnName() throws IOException {
    var records = read("id, name\n1, Robert\n2, Abduhla\n");

    assertThat(records)
        .containsExactly(Map.of("id", "1", "name", "Robert"), Map.of("id", "2", "name", "Abduhla"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1, Robert", "id, 2nd", "id,,name", ""})
  void givenInvalidHeaderRow_whenRead_thenThrowsInvalidDataFileException(String firstLine) {
    assertThatThrownBy(() -> read(firstLine + "\n2, Abduhla\n"))
        .isInstanceOf(InvalidDataFileException.class)
        .hasMessage(
            "The first line must be a header row of column names that each start with a letter,"
                + " but was: "
                + firstLine);
  }

  @Test
  void givenLongInvalidHeaderRow_whenRead_thenErrorMessageAbbreviatesLine() {
    var firstLine = "1," + "x".repeat(1_000);

    assertThatThrownBy(() -> read(firstLine + "\n"))
        .isInstanceOf(InvalidDataFileException.class)
        .hasMessageEndingWith("...")
        .hasMessageNotContaining(firstLine);
  }

  @Test
  void givenEmptyFile_whenRead_thenReturnsNoRecords() throws IOException {
    assertThat(read("")).isEmpty();
  }

  private List<Map<String, Object>> read(String content) throws IOException {
    return underTest
        .read(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
        .toList();
  }
}
