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

import java.io.IOException;
import java.nio.file.Path;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class JBangRunner {

  private volatile Boolean available = null;

  public static JBangRunner create() {
    return new JBangRunner();
  }

  public static JBangRunner disabled() {
    return new DisabledRunner();
  }

  public void exportLocalJar(Path srcFile, Path targetFile) throws IOException {
    if (!isJBangAvailable()) {
      return;
    }

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
    if (available == null) {
      synchronized (this) {
        if (available == null) {
          try {
            var proc = new ProcessBuilder("jbang", "--version").start();

            if (log.isDebugEnabled()) {
              var err = new String(proc.getErrorStream().readAllBytes());
              log.debug("JBang version: {}", err);
            }

            available = proc.waitFor() == 0;

          } catch (Exception e) {
            log.debug("JBang version check failed", e);
            available = false;
          }

          if (!available) {
            log.warn("JBang not found in PATH, JBang script preprocessing disabled");
          }
        }
      }
    }

    return available;
  }

  private static class DisabledRunner extends JBangRunner {

    @Override
    public void exportLocalJar(Path srcFile, Path targetFile) {
      // do nothing
    }

    @Override
    public boolean isJBangAvailable() {
      return false;
    }
  }
}
