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
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AddFuncCmdTest {

  private static final String UDF_NAME = "DummyFn";

  @TempDir private Path tempDir;

  private AddFuncCmd addFuncCmd;

  @BeforeEach
  void setup() {
    addFuncCmd = new AddFuncCmd();
    addFuncCmd.fnName = UDF_NAME;
  }

  @Test
  void givenExistingDirectory_whenInitProject_thenThrowsException() {
    // Initialize once
    addFuncCmd.addUdf(() -> tempDir);

    // When & Then - try to initialize again in same directory
    assertThatThrownBy(() -> addFuncCmd.addUdf(() -> tempDir))
        .isInstanceOf(FileAlreadyExistsException.class);
  }

  @Test
  void givenUdfName_whenAddUdf_thenCreatesFileWithCorrectName() {
    // When
    addFuncCmd.addUdf(() -> tempDir);

    // Then
    var udfFile = tempDir.resolve("functions").resolve(UDF_NAME + ".java");
    assertThat(udfFile).exists().isRegularFile();
  }

  @Test
  void givenUdfName_whenAddUdf_thenReplacesPlaceholderInContent() throws IOException {
    // When
    addFuncCmd.addUdf(() -> tempDir);

    // Then
    var udfFile = tempDir.resolve("functions").resolve(UDF_NAME + ".java");
    var content = Files.readString(udfFile);

    assertThat(content).contains("class " + UDF_NAME);
    assertThat(content).doesNotContain("__udfname__");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void givenAggregateFlag_whenAddUdf_thenCreatesCorrectUdfType(boolean aggregate)
      throws IOException {
    // Given
    addFuncCmd.aggregate = aggregate;

    // When
    addFuncCmd.addUdf(() -> tempDir);

    // Then
    var udfFile = tempDir.resolve("functions").resolve(UDF_NAME + ".java");
    assertThat(udfFile).exists();

    var content = Files.readString(udfFile);

    if (aggregate) {
      assertThat(content).contains("AggregateFunction");
    } else {
      assertThat(content).contains("ScalarFunction");
    }
  }

  @Test
  void givenNonExistentFunctionsDir_whenAddUdf_thenCreatesFunctionsDirectory() {
    // Given - ensure functions dir doesn't exist
    var functionsDir = tempDir.resolve("functions");
    assertThat(functionsDir).doesNotExist();

    // When
    addFuncCmd.addUdf(() -> tempDir);

    // Then
    assertThat(functionsDir).exists().isDirectory();
  }

  @Test
  void givenScalarUdf_whenAddUdf_thenContainsExpectedImports() throws IOException {
    // Given
    addFuncCmd.aggregate = false;

    // When
    addFuncCmd.addUdf(() -> tempDir);

    // Then
    var udfFile = tempDir.resolve("functions").resolve(UDF_NAME + ".java");
    var content = Files.readString(udfFile);

    assertThat(content)
        .contains("import org.apache.flink.table.functions.ScalarFunction")
        .contains("public String eval(");
  }

  @Test
  void givenAggregateUdf_whenAddUdf_thenContainsExpectedStructure() throws IOException {
    // Given
    addFuncCmd.aggregate = true;

    // When
    addFuncCmd.addUdf(() -> tempDir);

    // Then
    var udfFile = tempDir.resolve("functions").resolve(UDF_NAME + ".java");
    var content = Files.readString(udfFile);

    assertThat(content)
        .contains("import org.apache.flink.table.functions.AggregateFunction")
        .contains("public class " + UDF_NAME)
        .contains(UDF_NAME + ".Accumulator")
        .contains("public void accumulate(");
  }
}
