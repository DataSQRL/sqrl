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
package com.datasqrl.util;

import com.google.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

@Singleton
@Slf4j
public final class JBangRunner {

  public JBangRunner() {
    if (!isJBangAvailable()) {
      log.warn("JBang not found in PATH, JBang script preprocessing disabled");
    }
  }

  public void exportLocalJar(Path srcFile, Path targetFile) throws IOException {
    var cmdLine =
        new CommandLine("jbang")
            .addArgument("export")
            .addArgument("local")
            .addArgument("--force")
            .addArgument("--fresh")
            .addArgument("--output")
            .addArgument(targetFile.toString())
            .addArgument(srcFile.toString());

    // TODO: ferenc: add custom PrintStream for stderr of the executor to get JBang error
    var executor = DefaultExecutor.builder().get();
    executor.setExitValue(0);
    executor.execute(cmdLine);
  }

  public boolean isJBangAvailable() {
    try {
      var proc = new ProcessBuilder("jbang", "--version").start();

      if (log.isDebugEnabled()) {
        var err = new String(proc.getErrorStream().readAllBytes());
        log.debug("JBang version: {}", err);
      }

      return proc.waitFor() == 0;

    } catch (Exception e) {
      return false;
    }
  }
}
