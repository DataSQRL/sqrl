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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

public class TestOutputManager implements AutoCloseable {

  private static final String TEST_LOG_FILE = "test-execution.log";

  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final PrintStream logStream;

  @Getter private final ErrorCapturingStream errorCapturingStream;

  @SneakyThrows
  public TestOutputManager(Path rootDir) {
    var logsDir = rootDir.resolve("build/logs");
    Files.createDirectories(logsDir);

    var logFile = logsDir.resolve(TEST_LOG_FILE).toFile();
    var fileOutputStream = new FileOutputStream(logFile, true);
    errorCapturingStream = new ErrorCapturingStream(fileOutputStream);
    logStream = new PrintStream(errorCapturingStream);

    System.setOut(logStream);
    System.setErr(logStream);
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

  public List<String> getCapturedErrors() {
    return errorCapturingStream.getCapturedErrors();
  }

  public boolean hasErrors() {
    return errorCapturingStream.hasErrors();
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
  public void close() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    if (logStream != null) {
      logStream.close();
    }
  }
}
