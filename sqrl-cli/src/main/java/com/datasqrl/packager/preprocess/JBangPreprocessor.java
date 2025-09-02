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
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FilenameUtils;

@RequiredArgsConstructor(onConstructor_ = @Inject)
@Slf4j
public class JBangPreprocessor extends UdfManifestPreprocessor {

  private static final String DEPS_EXPR = "//DEPS";
  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([\\w.]+);");
  private static final Pattern CLASS_EXTENDS_PATTERN =
      Pattern.compile("public\\s+class\\s+(\\w+)\\s+extends\\s+(\\w+)", Pattern.DOTALL);

  private final JBangRunner jBangRunner;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (!jBangRunner.isJBangAvailable() || skipFile(file)) {
      return;
    }

    var content = readFileContent(file);
    var udfClassName = parseUdfClassName(file, content);
    if (udfClassName == null) {
      return;
    }

    try {
      var jarName = FilenameUtils.removeExtension(file.getFileName().toString()) + ".jar";
      var targetPath = ctx.libDir().resolve(jarName);

      jBangRunner.exportLocalJar(file, targetPath);
      createUdfManifest(udfClassName, jarName, ctx);

    } catch (ExecuteException e) {
      log.warn("JBang export failed with exit code: {} for file: {}", e.getExitValue(), file);
    } catch (IOException e) {
      log.warn("Failed to execute JBang export for file: " + file, e);
    }
  }

  @SneakyThrows
  private boolean skipFile(Path file) {
    if (!file.getFileName().toString().endsWith(".java")) {
      return true;
    }

    try (var lines = Files.lines(file)) {
      return lines.noneMatch(l -> l.startsWith(DEPS_EXPR));
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
      log.warn(
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
