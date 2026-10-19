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
package com.datasqrl.packager.preprocess;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.packager.FilePreprocessingPipeline;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CopyStaticDataPreprocessorTest {

  private final CopyStaticDataPreprocessor underTest = new CopyStaticDataPreprocessor();

  @TempDir Path tempDir;

  @Test
  void givenCsvFileWithHeader_whenProcess_thenCopiesItUnchangedAndWarns() throws IOException {
    var file = Files.writeString(tempDir.resolve("data.csv"), "id,name\n1,a\n");
    var errors = ErrorCollector.root();

    underTest.process(file, context(errors));

    assertThat(dataFile("data.csv")).hasSameBinaryContentAs(file);
    assertThat(warnings(errors))
        .singleElement()
        .asString()
        .contains("seems to start with a header row: id,name", "remove the header row");
  }

  @Test
  void givenCsvFileWithoutHeader_whenProcess_thenCopiesItWithoutWarning() throws IOException {
    var file = Files.writeString(tempDir.resolve("data.csv"), "1,a\n2,b\n");
    var errors = ErrorCollector.root();

    underTest.process(file, context(errors));

    assertThat(dataFile("data.csv")).hasSameBinaryContentAs(file);
    assertThat(warnings(errors)).isEmpty();
  }

  @Test
  void givenCompressedCsvFileWithHeader_whenProcess_thenCopiesItUnchangedAndWarns()
      throws IOException {
    var file = tempDir.resolve("data.csv.gz");
    try (var out = new GZIPOutputStream(Files.newOutputStream(file))) {
      out.write("id,name\n1,a\n".getBytes(StandardCharsets.UTF_8));
    }
    var errors = ErrorCollector.root();

    underTest.process(file, context(errors));

    assertThat(dataFile("data.csv.gz")).hasSameBinaryContentAs(file);
    assertThat(warnings(errors)).hasSize(1);
  }

  @Test
  void givenJsonlFile_whenProcess_thenCopiesItWithoutWarning() throws IOException {
    var file = Files.writeString(tempDir.resolve("data.jsonl"), "{\"id\":1}\n");
    var errors = ErrorCollector.root();

    underTest.process(file, context(errors));

    assertThat(dataFile("data.jsonl")).hasSameBinaryContentAs(file);
    assertThat(warnings(errors)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"id,name", "id;name", "id\tname", "\"id\",\"name\"", " id , name ", "﻿id,name"})
  void givenColumnNames_whenIsHeader_thenReturnsTrue(String line) {
    assertThat(CopyStaticDataPreprocessor.isHeader(line)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"1,a", "id,2", "-1,a", "id,,name", ""})
  void givenFieldNotStartingWithLetter_whenIsHeader_thenReturnsFalse(String line) {
    assertThat(CopyStaticDataPreprocessor.isHeader(line)).isFalse();
  }

  @Test
  void givenEmptyFile_whenIsHeader_thenReturnsFalse() {
    assertThat(CopyStaticDataPreprocessor.isHeader(null)).isFalse();
  }

  private FilePreprocessingPipeline.Context context(ErrorCollector errors) {
    return new FilePreprocessingPipeline.Context(
        tempDir, tempDir.resolve("build"), tempDir.resolve("lib"), tempDir.resolve("data"), errors);
  }

  private Path dataFile(String name) {
    return tempDir.resolve("data").resolve(name);
  }

  private static List<String> warnings(ErrorCollector errors) {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isWarning)
        .map(ErrorMessage::getMessage)
        .toList();
  }
}
