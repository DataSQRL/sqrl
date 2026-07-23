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

import com.datasqrl.config.PackageJson;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.JBangRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.StringJoiner;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JBangPreprocessor extends UdfManifestPreprocessor {

  public static final String JBANG_BUILD_TIME = "Build-Time";

  static final String JBANG_JAR_NAME = "jbang-udfs.jar";

  private static final FilenameAnalyzer JBANG_FILES = FilenameAnalyzer.of("java");

  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([\\w.]+);");
  private static final Pattern CLASS_EXTENDS_PATTERN =
      Pattern.compile("public\\s+class\\s+(\\w+)\\s+extends\\s+(\\w+)", Pattern.DOTALL);
  private static final String JBANG_SHEBANG = "///usr/bin/env jbang \"$0\" \"$@\" ; exit $?";

  private final Deque<JBangFileInfo> collectedFiles = new ArrayDeque<>();

  private final JBangRunner jBangRunner;
  private final PackageJson packageJson;

  private FilePreprocessingPipeline.Context ctx;
  private boolean skipJBangBuild = false;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (skipJBangBuild || !jBangRunner.isJBangAvailable() || !isJBangFile(file)) {
      return;
    }

    var jbangJarPath = file.getParent().resolve(JBANG_JAR_NAME);
    if (isJBangJarExistsAnValid(jbangJarPath)) {
      log.info(
          "Skip preprocessing JBang UDFs, as a valid JBang JAR is already present. To rebuild, delete the '{}' file from your project.",
          jbangJarPath);

      skipJBangBuild = true;
      return;
    }

    var content = readFileContent(file);

    var udfClass = parseUdfClass(file, content);
    if (udfClass == null) {
      return;
    }

    this.ctx = ctx;
    collectedFiles.addLast(udfClass);
  }

  @Override
  public void complete() {
    if (collectedFiles.isEmpty()) {
      return;
    }

    var targetPath = ctx.libDir().resolve(JBANG_JAR_NAME);
    var allClassNames = collectedFiles.stream().map(JBangFileInfo::udfClassName).toList();

    try {
      jBangRunner.exportFatJar(collectedFiles, targetPath);
      createUdfManifests(allClassNames, JBANG_JAR_NAME, ctx);

    } catch (ExecuteException e) {
      log.warn("JBang export failed with exit code: {}", e.getExitValue());
      return;

    } catch (IOException e) {
      log.warn("Failed to execute JBang export", e);
      return;
    }

    try {
      var srcPath = collectedFiles.peekFirst().file().getParent();
      Files.copy(targetPath, srcPath.resolve(JBANG_JAR_NAME));
    } catch (Exception e) {
      log.warn("Failed to copy JBang JAR to source directory", e);
    }
  }

  @SneakyThrows
  private boolean isJBangFile(Path file) {
    if (JBANG_FILES.analyze(file).isEmpty()) {
      return false;
    }

    var firstLine = Files.readAllLines(file).get(0).trim();
    return JBANG_SHEBANG.equals(firstLine);
  }

  private boolean isJBangJarExistsAnValid(Path jbangJar) {
    if (!Files.isRegularFile(jbangJar)) {
      return false;
    }

    try (var jar = new JarFile(jbangJar.toFile())) {
      var manifest = jar.getManifest();
      if (manifest == null) {
        return false;
      }

      var buildTimeValue = manifest.getMainAttributes().getValue(JBANG_BUILD_TIME);
      if (buildTimeValue == null) {
        return false;
      }

      var buildTime = Long.parseLong(buildTimeValue);
      var age = System.currentTimeMillis() - buildTime;
      var maxAge = packageJson.getCompilerConfig().getJBangJarMaxAge();

      return age >= 0 && age < maxAge.toMillis();

    } catch (IOException | NumberFormatException e) {
      log.warn("Failed to read build time from JBang JAR '{}', rebuilding...", jbangJar);
      return false;
    }
  }

  @SneakyThrows
  private String readFileContent(Path file) {
    return Files.readString(file);
  }

  private JBangFileInfo parseUdfClass(Path file, String content) {
    var classExtendsMatcher = CLASS_EXTENDS_PATTERN.matcher(content);
    var results = classExtendsMatcher.results().toList();
    if (results.isEmpty()) {
      log.info(
          "Skip preprocessing file {}, as it does not contain a 'public class' with an 'extends' statement",
          file);
      return null;
    }

    if (results.size() > 1) {
      log.warn(
          "Skip preprocessing file {}, as it contains multiple public classes that are extending another class",
          file);
      return null;
    }

    var classMatcherRes = results.get(0);

    var extendedClass = classMatcherRes.group(2); // group 2 is the extended class

    // Match against both canonical and simple class name
    var extendedUdfClass =
        FLINK_UDFS.stream()
            .filter(
                udfParentClass ->
                    udfParentClass.equals(extendedClass) || udfParentClass.endsWith(extendedClass))
            .findFirst();

    if (extendedUdfClass.isEmpty()) {
      log.warn(
          "Skip preprocessing file {}, as it does not extend a proper Flink UDF parent class",
          file);
      return null;
    }

    var className = new StringJoiner(".");

    // Extract package (optional in JBang files)
    var packageMatcher = PACKAGE_PATTERN.matcher(content);
    if (packageMatcher.find()) {
      className.add(packageMatcher.group(1));
    }

    className.add(classMatcherRes.group(1)); // group 1 is the class name

    return new JBangFileInfo(file, className.toString(), extendedUdfClass.get());
  }

  public record JBangFileInfo(Path file, String udfClassName, String parentUdfClassName) {}
}
