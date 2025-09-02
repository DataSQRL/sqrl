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

  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([\\w.]+);");
  private static final Pattern CLASS_PATTERN = Pattern.compile("public\\s+class\\s+(\\w+)");
  private static final Pattern EXTENDS_PATTERN = Pattern.compile("extends\\s+(\\w+)");
  private static final String DEPS_EXPR = "//DEPS";

  private final JBangRunner jBangRunner;

  @Override
  public void process(Path file, FilePreprocessingPipeline.Context ctx) {
    if (!jBangRunner.isJBangAvailable() || skipFile(file)) {
      return;
    }

    var content = readFileContent(file);
    if (!isFlinkUdfClass(content)) {
      log.debug("Skip preprocessing file, as it is not a valid Flink UDF: {}", file);
      return;
    }

    try {
      var jarName = getJarName(file);
      var targetPath = ctx.libDir().resolve(jarName);

      jBangRunner.exportLocalJar(file, targetPath);
      createManifestFromJavaFile(file, content, ctx);

    } catch (ExecuteException e) {
      log.warn("JBang export failed with exit code: {} for file: {}", e.getExitValue(), file);
    } catch (IOException e) {
      log.warn("Could not execute JBang for file: " + file, e);
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

  private boolean isFlinkUdfClass(String content) {
    var extendsMatcher = EXTENDS_PATTERN.matcher(content);
    if (!extendsMatcher.find()) {
      return false;
    }

    var processedUdf = extendsMatcher.group(1);

    // Match against both canonical and simple class name
    return FLINK_UDFS.stream()
        .anyMatch(
            udfParentClass ->
                udfParentClass.equals(processedUdf) || udfParentClass.endsWith(processedUdf));
  }

  private String getJarName(Path file) {
    return FilenameUtils.removeExtension(file.getFileName().toString()) + ".jar";
  }

  private void createManifestFromJavaFile(
      Path javaFile, String content, FilePreprocessingPipeline.Context ctx) {
    // Extract package (optional in JBang files)
    var packageMatcher = PACKAGE_PATTERN.matcher(content);
    var packageName = packageMatcher.find() ? packageMatcher.group(1) + "." : "";

    // Extract class name
    var classMatcher = CLASS_PATTERN.matcher(content);
    if (!classMatcher.find()) {
      log.debug("No public class found in file: {}", javaFile);
      return;
    }

    var className = classMatcher.group(1);
    var fullClassName = packageName + className;

    createUdfManifest(fullClassName, getJarName(javaFile), ctx);
  }
}
