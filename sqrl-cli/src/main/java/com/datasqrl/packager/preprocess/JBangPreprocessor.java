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

import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.util.JBangRunner;
import com.google.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;

@RequiredArgsConstructor(onConstructor_ = @Inject)
@Slf4j
public class JBangPreprocessor extends UdfManifestPreprocessor {

  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([\\w.]+);");
  private static final Pattern CLASS_EXTENDS_PATTERN =
      Pattern.compile("public\\s+class\\s+(\\w+)\\s+extends\\s+(\\w+)", Pattern.DOTALL);
  private static final Pattern FLINK_DEPS_PATTERN =
      Pattern.compile("^//DEPS\\s+org\\.apache\\.flink:", Pattern.MULTILINE);
  private static final String JBANG_SHEBANG = "///usr/bin/env jbang \"$0\" \"$@\" ; exit $?";
  static final String JBANG_JAR_NAME = "jbang-udfs.jar";

  private final JBangRunner jBangRunner;
  private final List<JBangFileInfo> collectedFiles = new ArrayList<>();
  private FilePreprocessingPipeline.Context ctx;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (!jBangRunner.isJBangAvailable() || !isJBangFile(file)) {
      return;
    }

    var content = readFileContent(file);

    validateNoFlinkDeps(file, content);

    var udfClassName = parseUdfClassName(file, content);
    if (udfClassName == null) {
      return;
    }

    this.ctx = ctx;
    collectedFiles.add(new JBangFileInfo(file, udfClassName));
  }

  @Override
  public void complete() {
    if (collectedFiles.isEmpty()) {
      return;
    }

    var targetPath = ctx.libDir().resolve(JBANG_JAR_NAME);
    var allPaths = collectedFiles.stream().map(JBangFileInfo::file).toList();
    var allClassNames = collectedFiles.stream().map(JBangFileInfo::udfClassName).toList();

    try {
      jBangRunner.exportFatJar(allPaths, targetPath);
      createUdfManifests(allClassNames, JBANG_JAR_NAME, ctx);
    } catch (ExecuteException e) {
      log.warn("JBang export failed with exit code: {}", e.getExitValue());
    } catch (IOException e) {
      log.warn("Failed to execute JBang export", e);
    }
  }

  private record JBangFileInfo(Path file, String udfClassName) {}

  private boolean isJavaFile(Path file) {
    return file.getFileName().toString().endsWith(".java");
  }

  @SneakyThrows
  private boolean isJBangFile(Path file) {
    if (!isJavaFile(file)) {
      return false;
    }
    var firstLine = Files.readAllLines(file).get(0).trim();
    return JBANG_SHEBANG.equals(firstLine);
  }

  private void validateNoFlinkDeps(Path file, String content) {
    if (FLINK_DEPS_PATTERN.matcher(content).find()) {
      throw new IllegalArgumentException(
          "File "
              + file
              + " declares a Flink //DEPS dependency. "
              + "Flink dependencies are provided automatically via classpath and must not be declared in //DEPS.");
    }
  }

  @SneakyThrows
  private String readFileContent(Path file) {
    return Files.readString(file);
  }

  private String parseUdfClassName(Path file, String content) {
    var classExtendsMatcher = CLASS_EXTENDS_PATTERN.matcher(content);
    var results = classExtendsMatcher.results().toList();
    if (results.isEmpty()) {
      log.debug(
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
    var extendsUdfClass =
        FLINK_UDFS.stream()
            .anyMatch(
                udfParentClass ->
                    udfParentClass.equals(extendedClass) || udfParentClass.endsWith(extendedClass));

    if (!extendsUdfClass) {
      log.warn(
          "Skip preprocessing file {}, as it does not extend a proper Flink UDF parent class",
          file);
      return null;
    }

    // Extract package (optional in JBang files)
    var packageMatcher = PACKAGE_PATTERN.matcher(content);
    var packageName = packageMatcher.find() ? packageMatcher.group(1) + "." : "";

    var className = classMatcherRes.group(1); // group 1 is the class name

    return packageName + className;
  }
}
