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

import static com.datasqrl.packager.preprocess.JBangPreprocessor.JBANG_BUILD_TIME;

import com.datasqrl.packager.preprocess.JBangPreprocessor.JBangFileInfo;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class JBangRunner {

  private static final String META_INF_PREFIX = "META-INF/";
  private static final String META_INF_SERVICES_PREFIX = META_INF_PREFIX + "services/";

  private volatile Boolean available = null;

  public static JBangRunner create() {
    return new JBangRunner();
  }

  public static JBangRunner disabled() {
    return new DisabledRunner();
  }

  public void exportFatJar(Deque<JBangFileInfo> jBangFiles, Path targetFile) throws IOException {
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

    jBangFiles.stream()
        .skip(1)
        .forEach(
            source -> {
              cmdLine.addArgument("--sources");
              cmdLine.addArgument(source.file().toString());
            });

    cmdLine.addArgument(jBangFiles.getFirst().file().toString());

    var executor = DefaultExecutor.builder().get();
    executor.setExitValue(0);
    executor.execute(cmdLine);

    try {
      rebuildFatJar(targetFile, List.copyOf(jBangFiles));
    } catch (IOException e) {
      log.warn("Failed to rebuild fat JAR", e);
    }
  }

  /**
   * Rebuilds the JBang fat JAR to:
   *
   * <ol>
   *   <li>add the current build time and generated UDF Service Loader descriptors,
   *   <li>remove signed-JAR signature files, and
   *   <li>deduplicate entries that otherwise trigger SecurityException or ZipException when Flink
   *       loads the UDF JAR.
   * </ol>
   */
  void rebuildFatJar(Path fatJar, List<JBangFileInfo> jBangFiles) throws IOException {
    var tempJar = fatJar.resolveSibling(fatJar.getFileName().toString() + ".tmp");
    var addedEntries = new HashSet<String>();
    var serviceEntries =
        jBangFiles.stream()
            .collect(
                Collectors.groupingBy(
                    file -> META_INF_SERVICES_PREFIX + file.parentUdfClassName(),
                    LinkedHashMap::new,
                    Collectors.mapping(JBangFileInfo::udfClassName, Collectors.toList())));

    try (var inJar = new JarFile(fatJar.toFile())) {
      var manifest = inJar.getManifest();
      if (manifest == null) {
        manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
      }
      manifest
          .getMainAttributes()
          .putValue(JBANG_BUILD_TIME, String.valueOf(System.currentTimeMillis()));

      try (var outJar = new JarOutputStream(Files.newOutputStream(tempJar), manifest)) {
        var entries = inJar.entries();
        while (entries.hasMoreElements()) {
          var entry = entries.nextElement();
          var name = entry.getName();

          if (entry.isDirectory() || name.equalsIgnoreCase("META-INF/MANIFEST.MF")) {
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
            if (serviceEntries.containsKey(name)) {
              appendServiceClasses(outJar, is.readAllBytes(), serviceEntries.get(name));
            } else {
              is.transferTo(outJar);
            }
          }
          outJar.closeEntry();
        }

        for (var serviceEntry : serviceEntries.entrySet()) {
          if (addedEntries.contains(serviceEntry.getKey())) {
            continue;
          }
          outJar.putNextEntry(new JarEntry(serviceEntry.getKey()));
          appendServiceClasses(outJar, new byte[0], serviceEntry.getValue());
          outJar.closeEntry();
        }
      }
    }

    var originalSize = Files.size(fatJar);
    Files.move(tempJar, fatJar, StandardCopyOption.REPLACE_EXISTING);
    var newSize = Files.size(fatJar);
    log.debug("Rebuilt fat JAR: {} -> {} bytes", originalSize, newSize);
  }

  private static void appendServiceClasses(
      JarOutputStream outJar, byte[] existingContent, List<String> udfClassNames)
      throws IOException {
    if (existingContent.length > 0) {
      outJar.write(existingContent);
      if (existingContent[existingContent.length - 1] != '\n') {
        outJar.write('\n');
      }
    }
    for (var udfClassName : udfClassNames) {
      outJar.write(udfClassName.getBytes(StandardCharsets.UTF_8));
      outJar.write('\n');
    }
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
    public void exportFatJar(Deque<JBangFileInfo> srcFiles, Path targetFile) {
      // do nothing
    }

    @Override
    public boolean isJBangAvailable() {
      return false;
    }
  }
}
