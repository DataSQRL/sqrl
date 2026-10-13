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

import com.datasqrl.lsp.SqrlLanguageServer;
import com.datasqrl.lsp.WorkDirectory;
import java.io.PrintStream;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import picocli.CommandLine.Command;

@Slf4j
@Command(
    name = "lsp",
    description = "Runs the SQRL language server for editors, communicating over stdin and stdout.")
public class LspCmd implements Runnable {

  private static final String LOG_CONFIG = "/log4j2-lsp.properties";

  @Override
  public void run() {
    // stdout carries the protocol, so everything else must go to stderr
    var protocolOut = System.out;
    System.setOut(new PrintStream(System.err, true));
    logToStderr();

    try {
      var workRoot = WorkDirectory.create(Path.of(System.getProperty("java.io.tmpdir")));
      SqrlLanguageServer.run(System.in, protocolOut, workRoot, System::exit);
    } catch (Exception e) {
      log.error("The SQRL language server failed", e);
      System.exit(1);
    }
  }

  private static void logToStderr() {
    try {
      var context = (LoggerContext) LogManager.getContext(false);
      context.setConfigLocation(LspCmd.class.getResource(LOG_CONFIG).toURI());
    } catch (Exception e) {
      System.err.println("Could not configure logging to stderr: " + e);
    }
  }
}
