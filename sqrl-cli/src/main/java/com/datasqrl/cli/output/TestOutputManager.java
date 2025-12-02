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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

@RequiredArgsConstructor
public class TestOutputManager implements AutoCloseable {

  private static final String TEST_LOG_FILE = "test-execution.log";

  private final Path rootDir;

  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private PrintStream logStream;

  @SneakyThrows
  public void init() {
    var logsDir = rootDir.resolve("build/logs");
    Files.createDirectories(logsDir);

    var logFile = logsDir.resolve(TEST_LOG_FILE).toFile();
    logStream = new PrintStream(new FileOutputStream(logFile, true));

    System.setOut(logStream);
    System.setErr(logStream);
  }

  public PrintStream getOriginalOut() {
    return originalOut;
  }

  public PrintStream getOriginalErr() {
    return originalErr;
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

  @Override
  public void close() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    if (logStream != null) {
      logStream.close();
    }
  }
}
