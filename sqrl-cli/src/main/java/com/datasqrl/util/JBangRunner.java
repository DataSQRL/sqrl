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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

  private volatile Boolean available = null;

  public static JBangRunner create() {
    return new JBangRunner();
  }

  public static JBangRunner withClasspath(String classpath) {
    return new JBangRunner() {
      @Override
      String resolveClasspath() {
        return classpath;
      }
    };
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

    var classpath = resolveClasspath();
    cmdLine.addArgument("--class-path");
    cmdLine.addArgument(classpath, false);

    for (int i = 1; i < srcFiles.size(); i++) {
      cmdLine.addArgument("--sources");
      cmdLine.addArgument(srcFiles.get(i).toString());
    }

    cmdLine.addArgument(srcFiles.get(0).toString());

    var executor = DefaultExecutor.builder().get();
    executor.setExitValue(0);
    executor.execute(cmdLine);

    try {
      stripClasspathEntries(targetFile, classpath);
    } catch (IOException e) {
      log.warn("Failed to strip classpath entries from fat JAR", e);
    }
  }

  // FIXME remove when https://github.com/jbangdev/jbang/issues/1749 is fixed (#1877)
  // JBang doesn't support provided scope, so --class-path entries get bundled into the fat JAR.
  // This strips them out, keeping only UDF classes and declared //DEPS.
  void stripClasspathEntries(Path fatJar, String classpath) throws IOException {
    var classpathEntryNames = collectJarEntryNames(classpath);
    if (classpathEntryNames.isEmpty()) {
      return;
    }

    var tempJar = fatJar.resolveSibling(fatJar.getFileName().toString() + ".tmp");
    try (var inJar = new JarFile(fatJar.toFile());
        var outJar = new JarOutputStream(Files.newOutputStream(tempJar))) {
      var entries = inJar.entries();
      while (entries.hasMoreElements()) {
        var entry = entries.nextElement();
        if (entry.getName().startsWith("META-INF/")
            || !classpathEntryNames.contains(entry.getName())) {
          outJar.putNextEntry(new JarEntry(entry.getName()));
          try (var is = inJar.getInputStream(entry)) {
            is.transferTo(outJar);
          }
          outJar.closeEntry();
        }
      }
    }

    var originalSize = Files.size(fatJar);
    Files.move(tempJar, fatJar, StandardCopyOption.REPLACE_EXISTING);
    var newSize = Files.size(fatJar);
    log.info("Stripped classpath entries from fat JAR: {} -> {} bytes", originalSize, newSize);
  }

  private Set<String> collectJarEntryNames(String classpath) throws IOException {
    var entryNames = new HashSet<String>();
    for (var cpEntry : classpath.split(File.pathSeparator)) {
      var cpPath = Path.of(cpEntry);
      if (Files.exists(cpPath) && cpPath.toString().endsWith(".jar")) {
        try (var jar = new JarFile(cpPath.toFile())) {
          var jarEntries = jar.entries();
          while (jarEntries.hasMoreElements()) {
            entryNames.add(jarEntries.nextElement().getName());
          }
        }
      }
    }
    return entryNames;
  }

  String resolveClasspath() {
    return resolveCliJarPath()
        .map(Path::toString)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Cannot resolve sqrl-cli.jar path. "
                        + "JBang UDF compilation requires the CLI fat JAR on the classpath to provide Flink dependencies."));
  }

  static Optional<Path> resolveCliJarPath() {
    try {
      var codeSource = JBangRunner.class.getProtectionDomain().getCodeSource();
      if (codeSource == null) {
        return Optional.empty();
      }

      var location = Path.of(codeSource.getLocation().toURI());
      if (!location.toString().endsWith(".jar")) {
        return Optional.empty();
      }

      return Optional.of(location);
    } catch (URISyntaxException e) {
      log.debug("Failed to resolve CLI jar path", e);
      return Optional.empty();
    }
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
