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
package com.datasqrl.cli.output;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestOutputManagerTest {

  @TempDir private Path tempDir;

  @Test
  void givenRedirectedStd_whenWritingOutput_thenStdoutIsLoggedAndStderrIsTeed() throws Exception {
    var originalOut = System.out;
    var originalErr = System.err;
    var consoleOut = new ByteArrayOutputStream();
    var consoleErr = new ByteArrayOutputStream();

    System.setOut(new PrintStream(consoleOut));
    System.setErr(new PrintStream(consoleErr));

    try (var outputManager = new TestOutputManager(tempDir)) {
      outputManager.redirectStd();

      System.out.println("hidden stdout");
      System.err.println("visible stderr");

      outputManager.restoreStd();
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }

    var log = Files.readString(tempDir.resolve("build/logs/test-execution.log"));

    assertThat(consoleOut.toString()).doesNotContain("hidden stdout");
    assertThat(consoleErr.toString()).contains("visible stderr");
    assertThat(log).contains("hidden stdout", "visible stderr");
  }
}
