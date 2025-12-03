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

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

@RequiredArgsConstructor
public class TestOutputManager implements AutoCloseable {

  private static final String TEST_LOG_FILE = "test-execution.log";

  private final Path rootDir;

  private PrintStream originalOut;
  private PrintStream originalErr;
  private PrintStream logStream;
  private ErrorCapturingStream errorCapturingStream;
  private OutputStream fileOutputStream;

  public void redirectStd() {
    originalOut = System.out;
    originalErr = System.err;

    initLogStream();

    System.setOut(logStream);
    System.setErr(logStream);
  }

  public void restoreStd() {
    if (originalOut != null) {
      System.setOut(originalOut);
      originalOut = null;
    }

    if (originalErr != null) {
      System.setErr(originalErr);
      originalErr = null;
    }
  }

  public void disableConsoleLogs() {
    var context = (LoggerContext) LogManager.getContext(false);
    var config = context.getConfiguration();

    for (var loggerConfig : config.getLoggers().values()) {
      loggerConfig.removeAppender("Console");
    }
    config.getRootLogger().removeAppender("Console");
    context.updateLoggers();
  }

  @SneakyThrows
  private void initLogStream() {
    if (logStream != null) {
      return;
    }

    var logsDir = rootDir.resolve("build/logs");
    Files.createDirectories(logsDir);

    var logFile = logsDir.resolve(TEST_LOG_FILE).toFile();
    fileOutputStream = new FileOutputStream(logFile, true);
    errorCapturingStream = new ErrorCapturingStream(fileOutputStream);
    logStream = new PrintStream(errorCapturingStream);
  }

  public List<String> getCapturedErrors() {
    if (errorCapturingStream == null) {
      return List.of();
    }
    return errorCapturingStream.getCapturedErrors();
  }

  public boolean hasErrors() {
    return errorCapturingStream != null && errorCapturingStream.hasErrors();
  }

  public void printCapturedErrors(OutputFormatter formatter) {
    if (!hasErrors()) {
      return;
    }

    formatter.sectionHeader("Captured Errors");
    for (var line : getCapturedErrors()) {
      formatter.info(line);
    }
  }

  @Override
  public void close() throws Exception {
    if (originalOut != null) {
      System.setOut(originalOut);
    }
    if (originalErr != null) {
      System.setErr(originalErr);
    }
    if (logStream != null) {
      logStream.close();
    }
  }
}
