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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class JBangRunner {

  private static final String META_INF_PREFIX = "META-INF/";

  private volatile Boolean available = null;

  public static JBangRunner create() {
    return new JBangRunner();
  }

  public static JBangRunner disabled() {
    return new DisabledRunner();
  }

  public void exportFatJar(List<Path> srcFiles, Path targetFile) throws IOException {
    if (!isJBangAvailable()) {
      return;
    }

    var cmdLine =
        new CommandLine("jbang")
            .addArgument("export")
            .addArgument("fatjar")
            .addArgument("--force")
            .addArgument("--fresh")
            .addArgument("--output")
            .addArgument(targetFile.toString());

    for (int i = 1; i < srcFiles.size(); i++) {
      cmdLine.addArgument("--sources");
      cmdLine.addArgument(srcFiles.get(i).toString());
    }

    cmdLine.addArgument(srcFiles.get(0).toString());

    var executor = DefaultExecutor.builder().get();
    executor.setExitValue(0);
    executor.execute(cmdLine);

    try {
      cleanFatJar(targetFile);
    } catch (IOException e) {
      log.warn("Failed to clean fat JAR", e);
    }
  }

  // Rebuild the JBang fat JAR to drop signed-jar signature files and deduplicate entries,
  // which otherwise trigger SecurityException / ZipException when Flink loads the UDF JAR.
  void cleanFatJar(Path fatJar) throws IOException {
    var tempJar = fatJar.resolveSibling(fatJar.getFileName().toString() + ".tmp");
    var addedEntries = new HashSet<String>();
    try (var inJar = new JarFile(fatJar.toFile());
        var outJar = new JarOutputStream(Files.newOutputStream(tempJar))) {
      var entries = inJar.entries();
      while (entries.hasMoreElements()) {
        var entry = entries.nextElement();
        var name = entry.getName();

        if (entry.isDirectory()) {
          continue;
        }
        if (isSignatureFile(name)) {
          continue;
        }
        if (!addedEntries.add(name)) {
          continue;
        }

        outJar.putNextEntry(new JarEntry(name));
        try (var is = inJar.getInputStream(entry)) {
          is.transferTo(outJar);
        }
        outJar.closeEntry();
      }
    }

    var originalSize = Files.size(fatJar);
    Files.move(tempJar, fatJar, StandardCopyOption.REPLACE_EXISTING);
    var newSize = Files.size(fatJar);
    log.debug("Cleaned fat JAR: {} -> {} bytes", originalSize, newSize);
  }

  private static boolean isSignatureFile(String entryName) {
    if (!entryName.startsWith(META_INF_PREFIX)) {
      return false;
    }
    var name = entryName.substring(META_INF_PREFIX.length());
    if (name.contains("/")) {
      return false;
    }
    return name.endsWith(".SF") || name.endsWith(".DSA") || name.endsWith(".RSA");
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
    public void exportFatJar(List<Path> srcFiles, Path targetFile) {
      // do nothing
    }

    @Override
    public boolean isJBangAvailable() {
      return false;
    }
  }
}
