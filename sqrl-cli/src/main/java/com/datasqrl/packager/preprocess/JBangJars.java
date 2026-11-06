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
package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Conventions of the JAR files that the {@link JBangPreprocessor} builds. The checks look up the
 * deployed JARs in the lib directory, which is cleaned before each compilation.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JBangJars {

  /** The manifest attribute of each UDF class that holds the SHA-256 digest of its script. */
  public static final String JBANG_FILE_SHA256 = "SHA-256-Digest";

  /** The name of a cached JBang JAR, and of the JBang JAR of the project in the lib directory. */
  static final String JBANG_JAR_NAME = "jbang-udfs.jar";

  private static final String INCLUDE_JBANG_JAR_NAME = "jbang-udfs-%s.jar";

  /**
   * Fails if the project and its included projects deployed different JBang UDFs with the same
   * class name, as only one of them can be loaded at runtime.
   */
  public static void failOnConflictingUdfs(
      Path libDir, List<String> includes, ErrorCollector errors) throws IOException {
    // UDF class name -> script digest -> JAR
    var udfJars = new TreeMap<String, Map<String, String>>();
    for (var jar : deployedJars(libDir, includes)) {
      for (var udf : udfDigests(libDir.resolve(jar)).entrySet()) {
        udfJars
            .computeIfAbsent(udf.getKey(), udfClass -> new LinkedHashMap<>())
            .putIfAbsent(udf.getValue(), jar);
      }
    }

    var conflicts =
        udfJars.entrySet().stream()
            .filter(udf -> udf.getValue().size() > 1)
            .map(
                udf ->
                    "%s (%s)".formatted(udf.getKey(), String.join(", ", udf.getValue().values())))
            .toList();

    errors.checkFatal(
        conflicts.isEmpty(),
        "Several projects define different JBang UDFs with the same class name: %s. Only one of"
            + " them can be loaded at runtime, so rename the classes or put them into different"
            + " packages.",
        String.join(", ", conflicts));
  }

  /**
   * Warns if the project and its included projects outside the project folder deployed several
   * JBang JARs, as each JAR bundles its own dependencies.
   */
  public static void warnIfBundledSeparately(
      Path libDir, List<String> externalIncludes, ErrorCollector errors) {

    var jars = deployedJars(libDir, externalIncludes);

    if (jars.size() > 1) {
      errors.warn(
          "Several projects bundle JBang UDFs into separate JARs: %s. If these JARs ship different"
              + " versions of the same dependency, it can cause failures at runtime.",
          String.join(", ", jars));
    }
  }

  static boolean isJBangJar(Path file) {
    return JBANG_JAR_NAME.equals(file.getFileName().toString());
  }

  /**
   * Included projects are preprocessed into the same lib directory as the project, so the JBang JAR
   * of an included project is suffixed with its namespace, e.g. {@code jbang-udfs-core.jar}.
   */
  static String libJarName(Optional<String> includeNamespace) {
    return includeNamespace.map(INCLUDE_JBANG_JAR_NAME::formatted).orElse(JBANG_JAR_NAME);
  }

  /** Returns the SHA-256 digest of the script of each UDF in the JBang JAR, by UDF class name. */
  static Map<String, String> udfDigests(Path jar) throws IOException {
    try (var jarFile = new JarFile(jar.toFile())) {
      var digests = new HashMap<String, String>();
      var manifest = jarFile.getManifest();
      if (manifest != null) {
        manifest
            .getEntries()
            .forEach(
                (udfClass, attributes) -> {
                  var digest = attributes.getValue(JBANG_FILE_SHA256);
                  if (digest != null) {
                    digests.put(udfClass, digest);
                  }
                });
      }

      return digests;
    }
  }

  private static List<String> deployedJars(Path libDir, List<String> includes) {
    var includeJars = includes.stream().map(INCLUDE_JBANG_JAR_NAME::formatted);

    return Stream.concat(Stream.of(JBANG_JAR_NAME), includeJars)
        .filter(jar -> Files.isRegularFile(libDir.resolve(jar)))
        .toList();
  }
}
